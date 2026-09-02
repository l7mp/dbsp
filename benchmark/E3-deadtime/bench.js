// E3: actuation under feedback dead time. The synthetic sibling of E2  - 
// E2 studies drift under plant disturbance, E3 studies actuation cost
// under a delayed watch.
//
// The plant that closes a reconciliation loop does not confirm a write the
// instant it lands: the apiserver admits it, a watch re-lists it, the event
// propagates. That round trip is a dead time of k steps. The plain
// desired-state Reconciler re-emits the whole outstanding correction U on
// every step until the echo confirms it, so a dead time of k turns one
// logical change into O(k) redundant applies. The SmithPredictor emits each
// correction exactly once - it retires the loop's own echoes by content
// against the delayed integral z^-1 U - so k appears nowhere in its
// actuation volume.
//
// The sweep variable is the loop's MODEL of the dead time, not the dead
// time itself: the plant runs at a fixed dead time --dt, and the
// SmithPredictor runs with an assumed window --k. A matched model (k = dt)
// actuates every correction exactly once; an under-modeled loop (k < dt)
// re-emits for the dt - k steps its model cannot cover, degenerating to
// the plain Reconciler as k -> 1 (the Reconciler IS the k = 1 endpoint of
// the sweep and stands in for it, since the predictor requires k >= 2).
//
// Two experiments:
//
//   --exp=amp   actuation amplification: a fixed churn workload runs for R
//               rounds at plant dead time dt; we count the entries the
//               plant applier receives (the actuation volume - every one is
//               a would-be apiserver write). Swept over the model k at
//               fixed dt: emission load vs model quality.
//
//   --exp=osc   the oscillation the theory warns about, and why Kubernetes
//               never shows it. A single disturbance hits at dead time dt
//               against three (plant, loop) configurations; we trace the
//               net weight the plant holds for the disturbed object.
//
//   --exp=rec   recovery under write loss: each round churns one pod and
//               measures the ticks until the new state lands in the plant,
//               with every correction entry dropped with probability
//               --drop (actuated but lost). The reconciler re-emits U
//               every step, so a lost write is re-actuated on the next
//               step; the smith loop retires corrections against its
//               k-step echo window, so re-assertion waits for the window
//               to drain: recovery latency grows with k - the price of
//               the silence the amp experiment buys.
//
// Usage:
//   dbsp bench.js --exp=amp --mode=<reconciled|smith> --dt=<DT> [--k=<K>]
//                 [--n=50] [--rounds=200] [--seed=42] [--out=results]
//   dbsp bench.js --exp=rec --mode=<reconciled|smith> --dt=<DT> [--k=<K>]
//                 [--drop=0.1] [--n=50] [--rounds=100]
//   dbsp bench.js --exp=osc --plant=<additive|idempotent>
//                 --mode=<reconciled|smith> [--dt=2] [--k=2] [--rounds=24]
//
// Synthetic and in-process: the dead time is injected by the plant, so no
// apiserver, no envtest, no k8s.

const minimist = require("minimist");
const fs = require("fs");
const fixtures = require("../lib/fixtures.js");
const endpoints = require("../lib/endpoints.js");
const { Collector, csvAppend, stableStringify } = require("../lib/measure.js");
const { Plant } = require("../lib/plant.js");

const argv = minimist(process.argv.slice(2), {
  string: ["exp", "mode", "plant", "out"],
  default: {
    exp: "amp",
    mode: "reconciled",
    plant: "",
    dt: 10,
    k: 0,
    n: 50,
    rounds: 200,
    drop: 0.1,
    seed: 42,
    out: "results",
  },
});

const cfg = {
  exp: argv.exp,
  mode: argv.mode, // circuit transform: reconciled | smith
  plant: argv.plant || argv.mode, // plant apply semantics; defaults to mode
  dt: Number(argv.dt), // the plant's actual feedback dead time
  k: Number(argv.k) || Number(argv.dt), // the Smith model's dead time (smith mode)
  n: Number(argv.n),
  rounds: Number(argv.rounds),
  drop: Number(argv.drop), // write-loss probability (rec experiment)
  seed: Number(argv.seed),
  out: argv.out,
};

if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}

// A seeded LCG, kept local so the sweep is reproducible without pulling in
// the noise library (E3 needs only a churn index).
function lcg(seed) {
  let s = seed >>> 0;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s / 4294967296;
  };
}
const rng = lcg(cfg.seed);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// A drain lets the single-threaded event loop run every pending circuit
// step and plant echo to completion before the driver publishes again.
// The circuit is tiny; this is generous.
const DRAIN_MS = 15;
const drain = () => sleep(DRAIN_MS);

// build compiles the (circuit, plant) pair: the plant runs at dead time
// dt, the smith loop models it with window k.
function build(dt) {
  const compiled = endpoints.compile({ case: "pair", mode: cfg.mode, smithK: cfg.k });
  const collector = new Collector(compiled.output);
  const plant = new Plant({
    mode: cfg.plant,
    keyFn: (d) => d.metadata.name,
    observed: compiled.observed,
    deadTime: dt,
  });
  plant.connect(collector);
  return { collector, plant };
}

// ---------------------------------------------------------------------------
// Experiment: actuation amplification.
// ---------------------------------------------------------------------------
async function amp() {
  const { plant } = build(cfg.dt);

  // Base load: N services, N pods, 1:1. Let it converge and flush every
  // echo so the loop starts quiescent (U = 0) before measurement.
  const b = fixtures.base("pair", cfg.n, 0);
  publish("pods", b.pods.map((d) => [d, 1]));
  publish("services", b.services.map((d) => [d, 1]));
  await drain();
  for (let i = 0; i <= cfg.dt; i++) {
    plant.tick();
    await drain();
  }

  const ip = new Map();
  for (let i = 1; i <= cfg.n; i++) {
    ip.set(i, fixtures.podIP(i));
  }

  const modelTag = cfg.mode === "smith" ? `k${cfg.k}` : "k1";
  const tracePath = `${cfg.out}/E3-trace-${cfg.mode}-dt${cfg.dt}-${modelTag}.csv`;
  if (fs.existsSync(tracePath)) {
    fs.rmSync(tracePath);
  }

  // Measured rounds: one pod IP churns per round (the background workload
  // that keeps every architecture doing work), then the plant's feedback
  // clock advances one tick, releasing the echoes whose dead time expired.
  let churnSeq = 0;
  const perRound = [];
  for (let t = 1; t <= cfg.rounds; t++) {
    const before = plant.applied;

    const i = 1 + Math.floor(rng() * cfg.n);
    const oldPod = fixtures.pod(`pod-${i}`, `app-${i}`, ip.get(i));
    const newIP = fixtures.podIP(cfg.n + 1 + churnSeq++);
    const newPod = fixtures.pod(`pod-${i}`, `app-${i}`, newIP);
    ip.set(i, newIP);
    publish("pods", [[oldPod, -1], [newPod, 1]]);
    await drain();

    plant.tick();
    await drain();

    const applied = plant.applied - before;
    perRound.push(applied);
    csvAppend(tracePath, ["round", "applied"], [t, applied]);
  }

  // Flush the last dt rounds' echoes so writes settle (not measured).
  for (let i = 0; i <= cfg.dt; i++) {
    plant.tick();
    await drain();
  }

  // Steady state: mean over the second half, past the k-round fill.
  const half = perRound.slice(Math.floor(perRound.length / 2));
  const meanApplied = half.reduce((a, x) => a + x, 0) / (half.length || 1);
  const totalApplied = perRound.reduce((a, x) => a + x, 0);
  // Minimal actuation is two entries per round (retract old, insert new).
  const minimal = 2;

  // The model column: the Reconciler is the k = 1 endpoint of the sweep.
  const model = cfg.mode === "smith" ? cfg.k : 1;
  csvAppend(
    `${cfg.out}/E3-amp-${cfg.mode}.csv`,
    ["dt", "k", "applied_per_round", "total_applied", "amplification"],
    [cfg.dt, model, meanApplied.toFixed(2), totalApplied, (meanApplied / minimal).toFixed(2)],
  );
  console.log(
    `E3 amp mode=${cfg.mode} dt=${cfg.dt} k=${model}: applied/round=${meanApplied.toFixed(2)} ` +
      `(x${(meanApplied / minimal).toFixed(2)} over minimal) total=${totalApplied}`,
  );
}

// ---------------------------------------------------------------------------
// Experiment: the oscillation, and why Kubernetes hides it.
// ---------------------------------------------------------------------------
//
// One spurious object D is inserted out of band at round 1, reported to the
// loop over the watch (dead time k). We trace the net weight the plant holds
// for D each round:
//
//   additive  + reconciled : the plant integrates every re-emission; with
//                            k>=2 the corrections stack and the weight rings
//                            (the z^2 = z - 1 pathology).
//   idempotent + reconciled: the set clamp (dist) absorbs the re-emissions;
//                            the weight never leaves {0,1}. No oscillation,
//                            only wasted applies. This is Kubernetes: apply
//                            is idempotent, so the pathology cannot manifest.
//   * + smith              : the disturbance is corrected once; the weight
//                            goes to 0 and stays.
async function osc() {
  const dt = cfg.dt;
  const { plant } = build(dt);

  // A small managed workload so the loop keeps stepping (re-emission of an
  // outstanding correction is only observable when some event clocks the
  // circuit during the dead-time window - exactly the mechanism the Smith
  // predictor guards against). Converge and flush.
  const n = cfg.n;
  const b = fixtures.base("pair", n, 0);
  publish("pods", b.pods.map((d) => [d, 1]));
  publish("services", b.services.map((d) => [d, 1]));
  await drain();
  for (let i = 0; i <= dt; i++) {
    plant.tick();
    await drain();
  }
  const ip = new Map();
  for (let i = 1; i <= n; i++) {
    ip.set(i, fixtures.podIP(i));
  }
  let churnSeq = 0;
  const churn = async () => {
    const i = 1 + Math.floor(rng() * n);
    const oldPod = fixtures.pod(`pod-${i}`, `app-${i}`, ip.get(i));
    const newIP = fixtures.podIP(n + 1 + churnSeq++);
    ip.set(i, newIP);
    publish("pods", [[oldPod, -1], [fixtures.pod(`pod-${i}`, `app-${i}`, newIP), 1]]);
    await drain();
  };

  // The disturbed object: a spurious Endpoints doc the loop never asked for.
  const D = {
    kind: "Endpoints",
    metadata: { name: "svc-ghost" },
    endpoints: ["10.9.9.9"],
  };
  const Dj = stableStringify(D);
  const weightOfD = () => {
    if (cfg.plant === "additive") {
      const rec = plant.aw.get(Dj);
      return rec ? rec.w : 0;
    }
    return plant.store.has("svc-ghost") ? 1 : 0;
  };

  const oscModel = cfg.mode === "smith" ? `k${cfg.k}` : "k1";
  const tracePath = `${cfg.out}/E3-osc-${cfg.plant}-${cfg.mode}-dt${dt}-${oscModel}.csv`;
  if (fs.existsSync(tracePath)) {
    fs.rmSync(tracePath);
  }

  const trace = [];
  for (let t = 0; t <= cfg.rounds; t++) {
    if (t === 1) {
      // Out-of-band insertion: the object appears in the plant and is
      // reported on the watch (delayed by the dead time).
      if (cfg.plant === "additive") {
        plant.aw.set(Dj, { doc: D, w: 1 });
      } else {
        plant.store.set("svc-ghost", { doc: D, json: Dj });
      }
      plant.report([[D, 1]]);
      await drain();
    }
    if (t >= 1) {
      await churn(); // keep the circuit stepping through the dead window
    }
    plant.tick();
    await drain();

    const w = weightOfD();
    trace.push(w);
    csvAppend(tracePath, ["round", "weight"], [t, w]);
  }

  const peak = trace.reduce((a, x) => Math.max(a, Math.abs(x)), 0);
  const settled = trace[trace.length - 1];
  const rang = trace.some((x) => x < 0) || peak > 1;
  console.log(
    `E3 osc plant=${cfg.plant} loop=${cfg.mode} dt=${dt} k=${cfg.mode === "smith" ? cfg.k : 1}: peak|w|=${peak} ` +
      `final=${settled} ${rang ? "RINGS" : "clamped"}`,
  );
}

// ---------------------------------------------------------------------------
// Experiment: recovery under write loss.
// ---------------------------------------------------------------------------
async function rec() {
  const { plant } = build(cfg.dt);
  plant.dropRng = lcg(cfg.seed + 1);

  // Base load, converged without loss so measurement starts clean.
  const b = fixtures.base("pair", cfg.n, 0);
  publish("pods", b.pods.map((d) => [d, 1]));
  publish("services", b.services.map((d) => [d, 1]));
  await drain();
  for (let i = 0; i <= cfg.dt + 2; i++) {
    plant.tick();
    await drain();
  }
  plant.dropP = cfg.drop;

  const ip = new Map();
  for (let i = 1; i <= cfg.n; i++) {
    ip.set(i, fixtures.podIP(i));
  }

  // The clock object: a dedicated pod churned on every wait tick so the
  // circuit keeps stepping through the recovery window (re-assertion is
  // only observable on a stepping circuit).
  let clkSeq = 0;
  let clkIP = fixtures.podIP(900000);
  publish("pods", [[fixtures.pod("pod-clk", "app-clk", clkIP), 1]]);
  await drain();
  const clock = async () => {
    const next = fixtures.podIP(900001 + clkSeq++);
    publish("pods", [
      [fixtures.pod("pod-clk", "app-clk", clkIP), -1],
      [fixtures.pod("pod-clk", "app-clk", next), 1],
    ]);
    clkIP = next;
    await drain();
  };

  const plantHasIP = (ipStr) => {
    for (const held of plant.store.values()) {
      if (held.json.includes(ipStr)) {
        return true;
      }
    }
    return false;
  };

  const model = cfg.mode === "smith" ? cfg.k : 1;
  const cap = 3 * model + 2 * cfg.dt + 20;
  const lat = [];
  let capped = 0;
  let churnSeq = 0;
  for (let t = 1; t <= cfg.rounds; t++) {
    // Round-robin over the pods, so a pending recovery is never re-churned.
    const i = 1 + (t % cfg.n);
    const oldPod = fixtures.pod(`pod-${i}`, `app-${i}`, ip.get(i));
    const newIP = fixtures.podIP(cfg.n + 1000 + churnSeq++);
    publish("pods", [[oldPod, -1], [fixtures.pod(`pod-${i}`, `app-${i}`, newIP), 1]]);
    ip.set(i, newIP);
    await drain();
    let ticks = 0;
    while (!plantHasIP(newIP) && ticks < cap) {
      await clock();
      plant.tick();
      await drain();
      ticks++;
    }
    if (ticks >= cap && !plantHasIP(newIP)) {
      capped++;
    }
    lat.push(ticks);
  }
  lat.sort((a, b) => a - b);
  const q = (p) => lat[Math.min(lat.length - 1, Math.floor(p * lat.length))];
  csvAppend(
    `${cfg.out}/E3-rec-${cfg.mode}.csv`,
    ["dt", "k", "drop", "median_ticks", "p95_ticks", "max_ticks", "dropped", "capped", "rounds"],
    [cfg.dt, model, cfg.drop, q(0.5), q(0.95), lat[lat.length - 1], plant.dropped, capped, cfg.rounds],
  );
  console.log(
    `E3 rec mode=${cfg.mode} dt=${cfg.dt} k=${model} drop=${cfg.drop}: ` +
      `median=${q(0.5)} p95=${q(0.95)} max=${lat[lat.length - 1]} ticks ` +
      `(dropped=${plant.dropped}, capped=${capped})`,
  );
}

async function run() {
  if (cfg.exp === "amp") {
    await amp();
  } else if (cfg.exp === "rec") {
    await rec();
  } else if (cfg.exp === "osc") {
    await osc();
  } else {
    throw new Error(`unknown experiment: ${cfg.exp}`);
  }
  console.log(`STATS ${JSON.stringify(runtime.stats())}`);
  exit(0);
}

run().catch((e) => {
  console.log("ERROR", e && e.stack ? e.stack : e);
  exit(1);
});
