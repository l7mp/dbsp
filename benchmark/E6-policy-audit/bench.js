// E6 driver: the policy-audit workloads, identical for every contender.
// The system under test runs out of process (the stock gatekeeper binary or
// the dpolicy controller, both spawned by run.sh against the plant
// plant); this driver only writes load and watches constraint statuses  - 
// one clock, one watch, one content predicate for every mode.
//
//   w1  detection latency: one compliance flip, flip -> status-visible
//   w2  idle cost: no churn for a window; status-write and sampler markers
//   w3  transient completeness: violations with lifetime L - detected?
//   w4  policy rollout: one new constraint over the converged inventory
//   w5  churn storm: violation ramp at a fixed event rate, per-event lag
//   w6  congestion: one flip corrects every constraint; heal time and the
//       epoch window for the request-log count of the SUT's writes
//
// Run through run.sh (which boots the harness and the contender); direct:
//
//	./js/bin/dbsp E6-policy-audit/bench.js --mode dpolicy --workloads w1 \
//	    --n 1000 --p 10 --reps 5 --out results/e6.csv

const minimist = require("minimist");
const { PolStack, sleep } = require("../lib/polstack.js");
const { csvAppend } = require("../lib/measure.js");

const argv = minimist(process.argv.slice(2), {
  default: {
    mode: "dpolicy",
    workloads: "w1",
    n: 1000,
    p: 10,
    reps: 5,
    density: 0.05,
    "idle-secs": 120,
    lifetimes: "5,15,30,60,120",
    rate: 5,
    events: 50,
    interval: 10,
    timeout: 600000,
    out: "E6-policy-audit/results/e6.csv",
    handshake: "/tmp/gk-serve/handshake.json",
    // Bootstrap, as dpolicy was started: "manual" (default) converges the
    // baseline with the loop open and lets the controller close it on
    // quiet; "auto" runs the loop closed from the first write (the
    // cold-start measurement). switch-quiet-ms is the controller's switch
    // period. A variant suffix labels its own line.
    bootstrap: "manual",
    "switch-quiet-ms": 20000,
    variant: "",
  },
});

const HEADER = ["mode", "workload", "n", "p", "rep", "param", "value_ms", "extra"];

function row(ctx, workload, rep, param, value, extra = "") {
  csvAppend(ctx.out, HEADER, [ctx.mode, workload, ctx.n, ctx.p, rep, param, value, extra]);
}

// --- workloads -------------------------------------------------------------

// w1: from the converged base, flip one pod into violation; measure
// flip-write -> first status carrying base+1.
async function w1(stack, ctx) {
  for (let rep = 0; rep < ctx.reps; rep++) {
    const probe = stack.violatingPod(`probe-w1-${rep}`);
    const t0 = performance.now();
    stack.addPod(probe);
    const at = await stack.waitCount("c0", ctx.base + 1, ctx.timeout, `w1 detect ${rep}`);
    row(ctx, "w1", rep, "", at - t0);
    stack.removePod(probe);
    await stack.waitCount("c0", ctx.base, ctx.timeout, `w1 clear ${rep}`);
  }
}

// w2: no churn for a window. The value is the number of constraint status
// writes observed (gatekeeper stamps every constraint every cycle; a delta
// engine writes nothing); CPU/RSS and API-server load come from the run.sh
// sampler and the apiserver request log, correlated via the epoch markers.
async function w2(stack, ctx) {
  stack.resetTrace();
  const start = Date.now();
  await sleep(ctx.idleSecs * 1000);
  const end = Date.now();
  row(ctx, "w2", 0, ctx.idleSecs, stack.statusEvents.length, `${start}:${end}`);
}

// w3: transient violations with lifetime L seconds. All reps of one
// lifetime run CONCURRENTLY: each rep owns a probe namespace and a probe
// constraint (an independent {0, 1} count), with the creations staggered
// across one audit interval so the phase sampling is uniform - sequential
// reps would phase-lock with the scanner's cycle, and a full sweep would
// take Σ(L)·reps of wall clock instead of Σ(L).
async function w3(stack, ctx) {
  const k = ctx.reps;
  stack.installProbes(k);
  for (let j = 0; j < k; j++) {
    await stack.waitCount(`c-probe-${j}`, 0, ctx.timeout, `w3 probe ${j} ready`);
  }

  for (const lifetime of ctx.lifetimes) {
    stack.resetTrace();
    const starts = [];
    await Promise.all(
      Array.from({ length: k }, (_, j) =>
        (async () => {
          await sleep((j / k) * ctx.interval * 1000);
          const probe = stack.violatingPod(`probe-w3-${lifetime}-${j}`, `probe-${j}`);
          starts[j] = performance.now();
          stack.addPod(probe);
          await sleep(lifetime * 1000);
          stack.removePod(probe);
        })(),
      ),
    );
    // One settle for the whole batch: a periodic scanner needs at most one
    // cycle after the last deletion to write its final counts.
    await sleep(ctx.interval * 1500 + 2000);
    for (let j = 0; j < k; j++) {
      const hit = stack.statusEvents.find(
        (e) => e.name === `c-probe-${j}` && e.totalViolations >= 1,
      );
      row(ctx, "w3", j, lifetime, hit ? hit.at - starts[j] : "", hit ? "detected" : "missed");
    }
  }

  stack.removeProbes();
  await sleep(2000);
}

// w4: policy rollout - one new constraint over the converged inventory;
// measure constraint-write -> first status carrying the full count.
async function w4(stack, ctx) {
  for (let rep = 0; rep < ctx.reps; rep++) {
    const name = `probe-c-${rep}`;
    const c = stack.constraint(name);
    const t0 = performance.now();
    publish(stack.constraintTopics.K8sRequiredLabels, [[c, 1]]);
    const at = await stack.waitCount(name, ctx.base, ctx.timeout, `w4 rollout ${rep}`);
    row(ctx, "w4", rep, "", at - t0);
    publish(stack.constraintTopics.K8sRequiredLabels, [[c, -1]]);
    await sleep(2000);
  }
}

// w5: a violation ramp at a fixed event rate; per-event detection lag is
// the first status event whose count reaches that event's level.
async function w5(stack, ctx) {
  stack.resetTrace();
  const probes = [];
  const starts = [];
  for (let i = 0; i < ctx.events; i++) {
    const probe = stack.violatingPod(`probe-w5-${i}`);
    probes.push(probe);
    starts.push(performance.now());
    stack.addPod(probe);
    await sleep(1000 / ctx.rate);
  }
  await stack.waitCount("c0", ctx.base + ctx.events, ctx.timeout, "w5 drain");
  for (let i = 0; i < ctx.events; i++) {
    const hit = stack.statusEvents.find(
      (e) => e.name === "c0" && e.totalViolations >= ctx.base + i + 1,
    );
    row(ctx, "w5", i, ctx.rate, hit ? hit.at - starts[i] : "", hit ? "" : "missed");
  }
  stack.podLoader.write(probes, -1);
  await stack.waitCount("c0", ctx.base, ctx.timeout, "w5 cleanup");
}

// w6: congestion, the window ladder's direct measure. One compliance flip
// changes the count of every constraint at once: as many status
// corrections as the corpus has constraints (the param column), queued
// behind the contender's client budget (run.sh throttles dpolicy to the
// realistic one). Per rep: the heal time until every required-labels
// constraint carries the new count, with the rep's epoch window in extra,
// so that summarize.py counts the SUT's writes in the request log (writes
// per correction: one is exactly-once actuation). Duplicates still queued
// behind the throttle land after the heal, so the window closes only after
// a quiet period.
const W6_QUIET_MS = 3000;
async function w6(stack, ctx) {
  for (let rep = 0; rep < ctx.reps; rep++) {
    const probe = stack.violatingPod(`probe-w6-${rep}`);
    const start = Date.now();
    const t0 = performance.now();
    stack.lastActivity = t0;
    stack.addPod(probe);
    const at = await stack.waitCountAll(ctx.base + 1, ctx.timeout, `w6 heal ${rep}`);
    await stack.settle(W6_QUIET_MS, ctx.timeout, `w6 settle ${rep}`);
    row(ctx, "w6", rep, stack.correctionsPerFlip, at - t0, `${start}:${Date.now()}`);
    stack.removePod(probe);
    await stack.waitCountAll(ctx.base, ctx.timeout, `w6 clear ${rep}`);
    await stack.settle(W6_QUIET_MS, ctx.timeout, `w6 clear settle ${rep}`);
  }
}

const WORKLOADS = { w1, w2, w3, w4, w5, w6 };

// --- main -------------------------------------------------------------------

async function main() {
  const ctx = {
    mode: String(argv.mode) + (argv.variant ? `-${argv.variant}` : ""),
    n: Number(argv.n),
    p: Number(argv.p),
    reps: Number(argv.reps),
    idleSecs: Number(argv["idle-secs"]),
    lifetimes: String(argv.lifetimes).split(",").map(Number).filter((x) => x > 0),
    rate: Number(argv.rate),
    events: Number(argv.events),
    interval: Number(argv.interval),
    timeout: Number(argv.timeout),
    out: String(argv.out),
  };
  // "none" runs the baseline convergence only (the cold-start points).
  const workloads = String(argv.workloads).split(",").map((w) => w.trim()).filter((w) => w && w !== "none");
  for (const w of workloads) {
    if (!WORKLOADS[w]) {
      throw new Error(`unknown workload ${w}`);
    }
  }

  const stack = new PolStack({ handshake: argv.handshake, pss: argv.pss !== false });
  if (argv["gk-cache"]) {
    stack.applySyncConfig();
  }

  stack.installCorpus(ctx.p);
  ctx.base = Math.max(1, Math.round(ctx.n * Number(argv.density)));
  stack.loadInventory(ctx.n, ctx.base);
  console.log(`[e6] mode=${ctx.mode} n=${ctx.n} p=${ctx.p} base=${ctx.base}: converging...`);
  const t0 = performance.now();
  const at = await stack.waitCount("c0", ctx.base, ctx.timeout, "baseline convergence");
  console.log(`[e6] baseline converged in ${Math.round(at - t0)}ms`);
  row(ctx, "preload", 0, "", at - t0);
  // A manual bootstrap: dpolicy (started with --bootstrap manual)
  // converged the baseline with the loop open and closes it by itself
  // once its inputs have been quiet for its switch period; wait out twice
  // that period of quiet before the workloads.
  if (String(argv.mode).startsWith("dpolicy") && argv.bootstrap !== "auto") {
    const t0 = performance.now();
    stack.lastActivity = t0;
    await stack.settle(2 * Number(argv["switch-quiet-ms"]), ctx.timeout, "bootstrap");
    console.log(`[e6] bootstrap: loop closed by the controller, quiet after ${Math.round(performance.now() - t0)}ms`);
  }

  for (const w of workloads) {
    console.log(`[e6] workload ${w}`);
    await WORKLOADS[w](stack, ctx);
  }

  console.log(`[e6] done; runtime errors: ${stack.runtimeErrors}`);
  console.log(`STATS ${JSON.stringify(runtime.stats())}`);
  exit(stack.runtimeErrors > 0 ? 1 : 0);
}

main().catch((err) => {
  console.log(`[e6] FAILED: ${err.message}`);
  exit(1);
});
