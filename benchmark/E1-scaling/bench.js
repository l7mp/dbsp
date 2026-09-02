// E1: per-delta latency and update size vs. base-config size, for one
// (case, mode, N) point. The sweep lives in run.sh; this file measures a
// single point and appends one summary row per invocation.
//
// Usage:
//   dbsp bench.js --case=<join|pair|fanout> --mode=<sotw|sotw-diff|incremental|reconciled>
//                 --n=<N> [--reps=20] [--warmup=3] [--indexed] [--out=results]
//                 [--timeout-ms=120000]

const minimist = require("minimist");
const fs = require("fs");
const fixtures = require("../lib/fixtures.js");
const endpoints = require("../lib/endpoints.js");
const { Collector, stats, csvAppend } = require("../lib/measure.js");
const { Plant } = require("../lib/plant.js");

const argv = minimist(process.argv.slice(2), {
  boolean: ["indexed"],
  string: ["case", "mode", "out"],
  default: {
    case: "pair",
    mode: "incremental",
    n: 100,
    reps: 20,
    warmup: 3,
    indexed: false,
    out: "results",
    "timeout-ms": 120000,
  },
});
const cfg = {
  kase: argv.case,
  mode: argv.mode,
  n: Number(argv.n),
  reps: Number(argv.reps),
  warmup: Number(argv.warmup),
  indexed: !!argv.indexed,
  out: argv.out,
  timeoutMs: Number(argv["timeout-ms"]),
};

// keyFn assigns plant identity to output documents.
const keyFn =
  cfg.kase === "join" ? (d) => `${d.svc}/${d.pod}` : (d) => d.metadata.name;

const compiled = endpoints.compile({ case: cfg.kase, mode: cfg.mode, indexed: cfg.indexed });
const collector = new Collector(compiled.output);
// Model 1 (sotw) ships the recomputed level: the plant overwrites
// wholesale. Model 2 (sotw-diff) leaves through the output adapter as a
// delta, so its plant applies deltas like the incremental modes.
const plant = new Plant({
  mode: cfg.mode === "sotw-diff" ? "incremental" : cfg.mode,
  keyFn: keyFn,
  observed: compiled.observed,
});
plant.connect(collector);

// --- Base load: one publish per input topic, so SotW recomputes once. ---
const spare = cfg.reps + cfg.warmup + 1;
const b = fixtures.base(cfg.kase, cfg.n, spare);
const baseMark = collector.mark();
const t0 = performance.now();
publish("pods", b.pods.map((d) => [d, 1]));
publish("services", b.services.map((d) => [d, 1]));
await collector.waitEntry(b.matches, 1, cfg.timeoutMs, "base load");
await plant.quiesce(collector, cfg.timeoutMs);
const baseMs = performance.now() - t0;
const baseWin = collector.since(baseMark);

// --- Measured deltas: add, await landing, retract, restore base. ---
async function oneDelta(j, record) {
  const d = fixtures.delta(cfg.kase, cfg.n, j);
  const mark = collector.mark();
  const w0 = { writes: plant.writes, bytes: plant.writeBytes };
  const ta = performance.now();
  publish(d.topic, [[d.doc, 1]]);
  // Landing means the new doc shows up (in the snapshot or the delta).
  await collector.waitEntry(d.matches, 1, cfg.timeoutMs, `add ${j}`);
  const ms = performance.now() - ta;
  await plant.quiesce(collector, cfg.timeoutMs);
  if (record) {
    record({
      ms: ms,
      win: collector.since(mark),
      writes: plant.writes - w0.writes,
      writeBytes: plant.writeBytes - w0.bytes,
    });
  }

  // Retract to restore the base config.
  publish(d.topic, [[d.doc, -1]]);
  if (cfg.mode === "sotw") {
    // Model 1 ships levels: the retraction lands as the next full
    // state, never as a -1 entry.
    await collector.waitSteps(1, cfg.timeoutMs, `retract ${j}`);
  } else {
    await collector.waitEntry(d.matches, -1, cfg.timeoutMs, `retract ${j}`);
  }
  await plant.quiesce(collector, cfg.timeoutMs);
}

for (let j = 1; j <= cfg.warmup; j++) {
  await oneDelta(j, null);
}

const lat = [];
const wins = [];
for (let j = cfg.warmup + 1; j <= cfg.warmup + cfg.reps; j++) {
  await oneDelta(j, (r) => {
    lat.push(r.ms);
    wins.push(r);
  });
}

// --- Report. ---
const st = stats(lat);
const meanOf = (f) => wins.reduce((a, r) => a + f(r), 0) / (wins.length || 1);
const variant = cfg.indexed ? `${cfg.kase}-idx` : cfg.kase;

if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}
csvAppend(
  `${cfg.out}/E1-${variant}-${cfg.mode}.csv`,
  [
    "n", "reps",
    "median_ms", "p95_ms", "mean_ms", "min_ms", "max_ms",
    "delta_entries", "delta_bytes", "delta_writes", "delta_write_bytes",
    "base_ms", "base_entries", "base_bytes",
  ],
  [
    cfg.n, st.n,
    st.median.toFixed(3), st.p95.toFixed(3), st.mean.toFixed(3), st.min.toFixed(3), st.max.toFixed(3),
    meanOf((r) => r.win.entries).toFixed(1),
    meanOf((r) => r.win.bytes).toFixed(0),
    meanOf((r) => r.writes).toFixed(1),
    meanOf((r) => r.writeBytes).toFixed(0),
    baseMs.toFixed(1), baseWin.entries, baseWin.bytes,
  ],
);
for (let i = 0; i < lat.length; i++) {
  csvAppend(
    `${cfg.out}/raw-E1.csv`,
    ["case", "mode", "indexed", "n", "rep", "ms"],
    [cfg.kase, cfg.mode, cfg.indexed ? 1 : 0, cfg.n, i + 1, lat[i].toFixed(3)],
  );
}

console.log(
  `E1 case=${variant} mode=${cfg.mode} n=${cfg.n}: ` +
    `median=${st.median.toFixed(3)}ms p95=${st.p95.toFixed(3)}ms ` +
    `delta_bytes=${meanOf((r) => r.win.bytes).toFixed(0)} ` +
    `delta_writes=${meanOf((r) => r.writes).toFixed(1)} base=${baseMs.toFixed(0)}ms`,
);
console.log(`STATS ${JSON.stringify(runtime.stats())}`);
exit();
