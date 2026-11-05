// E2: state drift under plant disturbance, for one (mode, noise-rate)
// point. The controller runs the pair-case endpoints pipeline (N services,
// one pod each) over an in-process plant; a seeded noise source tampers
// with the plant behind the controller's back; the drift between the plant
// and the analytically known desired state is sampled every round.
//
// No controller but the reconciled one watches the Endpoints it writes, so
// the tampering reaches only M4. The other architectures react only to
// their inputs: a background pod IP churn, once every --churn-period rounds
// with a jitter of a quarter period, makes every architecture write. M1
// rewrites the whole level on each churn, which repairs every tampered
// object at once; M2 and M3 write the churned service only.
//
// Round structure: (1) churn one pod IP if the round is a churn round and
// let the controller react, (2) inject the round's disturbances, letting
// the reconciled loop react to each watch report, (3) sample drift.
//
// Usage:
//   dbsp bench.js --mode=<sotw|sotw-diff|incremental|reconciled> [--n=1000]
//                 [--rounds=1000] [--noise=0.2] [--churn-period=50]
//                 [--burst=0] [--seed=42] [--out=results] [--timeout-ms=60000]
//
// With --burst=B, continuous noise is disabled; B disturbances hit at
// round rounds/2 and the trace records the recovery (MTTR) instead.

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
    rounds: 1000,
    noise: 0.2,
    "churn-period": 50,
    burst: 0,
    seed: 42,
    out: "results",
    "timeout-ms": 60000,
  },
});
const cfg = {
  mode: argv.mode,
  n: Number(argv.n),
  rounds: Number(argv.rounds),
  noise: Number(argv.noise),
  churnPeriod: Number(argv["churn-period"]),
  burst: Number(argv.burst),
  seed: Number(argv.seed),
  out: argv.out,
  timeoutMs: Number(argv["timeout-ms"]),
};
// Separate streams for the noise and the churn, so every architecture sees
// the same disturbances and the same churn schedule.
const noiseRng = mulberry32(cfg.seed);
const churnRng = mulberry32(cfg.seed + 1);

const compiled = endpoints.compile({ case: "pair", mode: cfg.mode, indexed: true });
const collector = new Collector(compiled.output);
// Model 2 leaves through the output adapter as a delta, so its plant
// applies deltas like the incremental modes.
const plant = new Plant({
  mode: cfg.mode === "sotw-diff" ? "incremental" : cfg.mode,
  keyFn: (d) => d.metadata.name,
  observed: compiled.observed,
});
plant.connect(collector);

// --- Input mirror and desired-state oracle. ---
// pair case: svc-i selects pod-i; the desired state is one Endpoints doc
// per service carrying its pod's current IP.
const ip = new Map(); // i -> current pod IP
for (let i = 1; i <= cfg.n; i++) {
  ip.set(i, fixtures.podIP(i));
}
const managed = [];
for (let i = 1; i <= cfg.n; i++) {
  managed.push(`svc-${i}`);
}
managed.sort();

function oracle() {
  const m = new Map();
  for (let i = 1; i <= cfg.n; i++) {
    const doc = {
      kind: "Endpoints",
      metadata: { name: `svc-${i}` },
      endpoints: [ip.get(i)],
    };
    m.set(`svc-${i}`, stableStringify(doc));
  }
  return m;
}

// --- Base load. ---
const b = fixtures.base("pair", cfg.n, 0);
publish("pods", b.pods.map((d) => [d, 1]));
publish("services", b.services.map((d) => [d, 1]));
await collector.waitEntry(b.matches, 1, cfg.timeoutMs, "base load");

const drift0 = plant.drift(oracle());
if (drift0 !== 0) {
  throw new Error(`plant did not converge after base load: drift=${drift0}`);
}

// --- Rounds. ---
let churnSeq = 0;
async function churn() {
  const i = 1 + Math.floor(churnRng() * cfg.n);
  const oldPod = fixtures.pod(`pod-${i}`, `app-${i}`, ip.get(i));
  const newIP = fixtures.podIP(cfg.n + 1 + churnSeq++);
  const newPod = fixtures.pod(`pod-${i}`, `app-${i}`, newIP);
  ip.set(i, newIP);
  publish("pods", [
    [oldPod, -1],
    [newPod, 1],
  ]);
  await collector.waitSteps(1, cfg.timeoutMs, "churn step");
}

// nextChurn draws the round of the next churn: one period on, jittered by
// up to a quarter period either way.
function nextChurn(t) {
  const j = Math.floor(cfg.churnPeriod / 4);
  return t + cfg.churnPeriod - j + Math.floor(churnRng() * (2 * j + 1));
}

// disturb injects one disturbance and, in reconciled mode, lets the loop
// react to the watch report; heal collects the time from the injection to
// the correction step.
const heal = [];
async function disturb() {
  const kind = noise.pick(noiseRng, ["delete", "corrupt", "insert"]);
  const t0 = performance.now();
  if (!noise.inject(plant, noiseRng, kind, managed)) {
    return false;
  }
  if (cfg.mode === "reconciled") {
    await collector.waitSteps(1, cfg.timeoutMs, "disturbance report");
    heal.push(performance.now() - t0);
  }
  return true;
}

// noiseCount draws the number of disturbances for one round.
function noiseCount() {
  let k = Math.floor(cfg.noise);
  if (noiseRng() < cfg.noise - k) {
    k++;
  }
  return k;
}

const variant = cfg.burst > 0 ? "burst" : `l${cfg.noise}`;
const tracePath = `${cfg.out}/E2-trace-${cfg.mode}-${variant}.csv`;
if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}
if (fs.existsSync(tracePath)) {
  fs.rmSync(tracePath);
}

const burstRound = Math.floor(cfg.rounds / 2);
let cumNoise = 0;
let churnAt = nextChurn(0);
const drifts = [];
let healedRound = -1;

for (let t = 1; t <= cfg.rounds; t++) {
  let churned = 0;
  if (t === churnAt) {
    await churn();
    churned = 1;
    churnAt = nextChurn(t);
  }

  if (cfg.burst > 0) {
    if (t === burstRound) {
      for (let k = 0; k < cfg.burst; k++) {
        if (await disturb()) {
          cumNoise++;
        }
      }
    }
  } else {
    const k = noiseCount();
    for (let j = 0; j < k; j++) {
      if (await disturb()) {
        cumNoise++;
      }
    }
  }

  const d = plant.drift(oracle());
  drifts.push(d);
  if (cfg.burst > 0 && t >= burstRound && healedRound < 0 && d === 0) {
    healedRound = t;
  }
  csvAppend(tracePath, ["round", "drift", "cum_noise", "churn"], [t, d, cumNoise, churned]);
}

// --- Summary. ---
const half = drifts.slice(Math.floor(drifts.length / 2));
const meanDrift = half.reduce((a, x) => a + x, 0) / (half.length || 1);
const maxDrift = drifts.reduce((a, x) => Math.max(a, x), 0);

if (cfg.burst > 0) {
  const mttr = healedRound < 0 ? -1 : healedRound - burstRound;
  csvAppend(
    `${cfg.out}/E2-burst-summary.csv`,
    ["mode", "n", "burst", "burst_round", "healed_round", "mttr_rounds"],
    [cfg.mode, cfg.n, cfg.burst, burstRound, healedRound, mttr],
  );
  console.log(
    `E2 mode=${cfg.mode} burst=${cfg.burst}: mttr=${mttr < 0 ? "never" : mttr + " round(s)"} max_drift=${maxDrift}`,
  );
} else {
  csvAppend(
    `${cfg.out}/E2-steady-${cfg.mode}.csv`,
    ["lambda", "n", "rounds", "churn_period", "mean_drift", "max_drift", "cum_noise"],
    [cfg.noise, cfg.n, cfg.rounds, cfg.churnPeriod, meanDrift.toFixed(2), maxDrift, cumNoise],
  );
  console.log(
    `E2 mode=${cfg.mode} noise=${cfg.noise} churn_period=${cfg.churnPeriod}: ` +
      `mean_drift(2nd half)=${meanDrift.toFixed(2)} max=${maxDrift} injected=${cumNoise}`,
  );
}
if (heal.length > 0) {
  const h = stats(heal);
  csvAppend(
    `${cfg.out}/E2-heal.csv`,
    ["mode", "n", "lambda", "count", "median_ms", "p95_ms", "max_ms"],
    [cfg.mode, cfg.n, cfg.noise, h.n, h.median.toFixed(3), h.p95.toFixed(3), h.max.toFixed(3)],
  );
  console.log(`E2 heal mode=${cfg.mode} n=${cfg.n}: median=${h.median.toFixed(3)}ms p95=${h.p95.toFixed(3)}ms`);
}
console.log(`STATS ${JSON.stringify(runtime.stats())}`);
exit();
