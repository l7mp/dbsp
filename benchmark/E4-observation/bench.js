// E4: observation is not correction. Every architecture watches the
// Endpoints it writes, so every out-of-band disturbance reaches every
// controller as a watch event and triggers a step; what the step does with
// it is the architecture. For one (mode, N) point, the driver disturbs one
// managed object at a time, waits until the controller and the plant have
// gone quiet, and records per disturbance whether the object is back to
// its desired content, how many circuit steps the disturbance triggered,
// the time the circuit spent stepping, and how many objects were written.
//
// The plant reports every change on the watch, the controller's own
// writes included (the echo), as a Kubernetes watch does; a write that
// leaves an object unchanged produces no event.
//
//   sotw:        the watch event triggers a full recompute and a rewrite of
//                the whole level, which repairs the object; the repair
//                comes back on the watch and triggers a second recompute.
//   sotw-diff:   the watch event triggers a full recompute, whose delta
//                against the last output is empty: nothing is written.
//   incremental: the watch event triggers a step whose output delta is
//                empty: nothing is written.
//   reconciled:  the watch event enters the feedback; the correction of the
//                one object is written, and its echo settles the loop.
//
// Usage:
//   dbsp bench.js --mode=<sotw|sotw-diff|incremental|reconciled> --n=<N>
//                 [--reps=30] [--quiet-ms=50] [--seed=42] [--out=results]
//                 [--timeout-ms=120000]

const minimist = require("minimist");
const fs = require("fs");
const fixtures = require("../lib/fixtures.js");
const endpoints = require("../lib/endpoints.js");
const { Collector, csvAppend, mulberry32, stableStringify, stats } = require("../lib/measure.js");
const { Plant } = require("../lib/plant.js");
const noise = require("../lib/noise.js");

const argv = minimist(process.argv.slice(2), {
  string: ["mode", "out"],
  default: {
    mode: "reconciled",
    n: 1000,
    reps: 30,
    "quiet-ms": 50,
    seed: 42,
    out: "results",
    "timeout-ms": 120000,
  },
});
const cfg = {
  mode: argv.mode,
  n: Number(argv.n),
  reps: Number(argv.reps),
  quietMs: Number(argv["quiet-ms"]),
  seed: Number(argv.seed),
  out: argv.out,
  timeoutMs: Number(argv["timeout-ms"]),
};
const rng = mulberry32(cfg.seed);

const compiled = endpoints.compile({ case: "pair", mode: cfg.mode, indexed: true, watch: true });
const circuitName = `endpoints-pair-${cfg.mode}`;
const collector = new Collector(compiled.output);
const plant = new Plant({
  mode: cfg.mode === "sotw-diff" ? "incremental" : cfg.mode,
  keyFn: (d) => d.metadata.name,
  observed: compiled.observed,
  echo: true,
});
plant.connect(collector);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// counters reads the circuit's step counters from the runtime stats.
function counters() {
  const c = runtime.stats().counters;
  return {
    started: c[`circuit.${circuitName}.steps_started`] || 0,
    steps: c[`circuit.${circuitName}.steps`] || 0,
    ns: c[`circuit.${circuitName}.step_ns`] || 0,
  };
}

// quiet resolves once no step has run or started for a quiet window, and
// returns the time the last step was seen to finish (null if no step ran
// since since.steps). The window is quietMs plus twice the longest step
// seen so far: after a long step the plant applies a large output on the
// JS thread before its echo can start the next step.
let longestStepMs = 0;
async function quiet(since) {
  const deadline = performance.now() + cfg.timeoutMs;
  let last = counters();
  let lastChange = performance.now();
  let lastStepEnd = last.steps > since.steps ? lastChange : null;
  for (;;) {
    await sleep(1);
    const now = performance.now();
    const c = counters();
    if (c.started !== last.started || c.steps !== last.steps) {
      if (c.steps !== last.steps) {
        lastStepEnd = now;
        longestStepMs = Math.max(longestStepMs, (c.ns - last.ns) / 1e6 / (c.steps - last.steps));
      }
      last = c;
      lastChange = now;
    } else if (c.started === c.steps && now - lastChange >= cfg.quietMs + 2 * longestStepMs) {
      return lastStepEnd;
    }
    if (now > deadline) {
      throw new Error(`timeout waiting for the circuit to go quiet after ${cfg.timeoutMs}ms`);
    }
  }
}

// --- Base load. ---
const b = fixtures.base("pair", cfg.n, 0);
publish("pods", b.pods.map((d) => [d, 1]));
publish("services", b.services.map((d) => [d, 1]));
await collector.waitEntry(b.matches, 1, cfg.timeoutMs, "base load");
await quiet(counters());

// The desired content of every managed object, fixed: nothing churns.
const desired = new Map();
const managed = [];
for (let i = 1; i <= cfg.n; i++) {
  const doc = { kind: "Endpoints", metadata: { name: `svc-${i}` }, endpoints: [fixtures.podIP(i)] };
  desired.set(`svc-${i}`, stableStringify(doc));
  managed.push(`svc-${i}`);
}
for (const [key, json] of desired) {
  const cur = plant.store.get(key);
  if (!cur || cur.json !== json) {
    throw new Error(`plant did not converge after base load at ${key}`);
  }
}

// --- Disturbances: delete, corrupt and insert in turn, each on an object
// the plant holds in its desired content. ---
const kinds = ["delete", "corrupt", "insert"];
const rows = [];
for (let r = 0; r < cfg.reps; r++) {
  const kind = kinds[r % kinds.length];
  let victim;
  do {
    victim = managed[Math.floor(rng() * managed.length)];
  } while (!plant.store.has(victim) || plant.store.get(victim).json !== desired.get(victim));
  const c0 = counters();
  const w0 = plant.writes;
  const t0 = performance.now();
  const key = noise.inject(plant, rng, kind, [victim]);
  const stepEnd = await quiet(c0);
  const c1 = counters();
  const cur = plant.store.get(key);
  const healed = kind === "insert" ? !cur : !!cur && cur.json === desired.get(key);
  rows.push({
    kind: kind,
    healed: healed,
    steps: c1.steps - c0.steps,
    stepMs: (c1.ns - c0.ns) / 1e6,
    writes: plant.writes - w0,
    reactMs: stepEnd === null ? 0 : stepEnd - t0,
  });
}

// --- Report. ---
const mean = (f) => rows.reduce((a, r) => a + f(r), 0) / (rows.length || 1);
const st = stats(rows.map((r) => r.stepMs));
const rt = stats(rows.map((r) => r.reactMs));
if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}
csvAppend(
  `${cfg.out}/E4-${cfg.mode}.csv`,
  [
    "n", "reps", "healed_frac", "steps_per", "writes_per",
    "step_ms_median", "step_ms_p95", "react_ms_median", "react_ms_p95",
  ],
  [
    cfg.n, rows.length,
    mean((r) => (r.healed ? 1 : 0)).toFixed(2),
    mean((r) => r.steps).toFixed(2),
    mean((r) => r.writes).toFixed(1),
    st.median.toFixed(3), st.p95.toFixed(3), rt.median.toFixed(3), rt.p95.toFixed(3),
  ],
);
for (const r of rows) {
  csvAppend(
    `${cfg.out}/raw-E4.csv`,
    ["mode", "n", "kind", "healed", "steps", "step_ms", "writes", "react_ms"],
    [cfg.mode, cfg.n, r.kind, r.healed ? 1 : 0, r.steps, r.stepMs.toFixed(3), r.writes, r.reactMs.toFixed(3)],
  );
}
console.log(
  `E4 mode=${cfg.mode} n=${cfg.n}: healed=${mean((r) => (r.healed ? 1 : 0)).toFixed(2)} ` +
    `steps/dist=${mean((r) => r.steps).toFixed(2)} writes/dist=${mean((r) => r.writes).toFixed(1)} ` +
    `step_ms median=${st.median.toFixed(3)} p95=${st.p95.toFixed(3)} react_ms median=${rt.median.toFixed(3)}`,
);
console.log(`STATS ${JSON.stringify(runtime.stats())}`);
exit();
