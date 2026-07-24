// smith.js -- The Smith dead-time compensator, observable on a real circuit.
//
// Two copies of the in-memory Endpoints controller run side by side on the
// same inputs: one closed with the plain desired-state Reconciler (which
// re-emits the outstanding correction U on every step until the feedback
// confirms it), one with the SmithPredictor (window K=2: every emission is
// followed by at least one silent step, and the loop compares the feedback
// against its prediction instead of the raw observation). A tiny JS plant
// applies each loop's emissions to a set-based store and echoes the state
// change back on the loop's observed topic after a dead time.
//
// The script drives the note's worked run (doc/pres/smith.org) and prints
// the per-event log: watch the "rec out" column re-emit U when an unrelated
// input event fires during the dead-time window, while "smith out" stays
// silent (the correction rides the window); then watch the disturbance
// arrive as news, corrected at first sight, and the repair's echo meet its
// window expiry in silence.
//
// Run:  ./js/bin/dbsp js/examples/smith.js

const DEAD_TIME_MS = 150; // the plant's feedback dead time

const PIPELINE = [
  { "@join": [
      { "@eq": ["$.pods.metadata.labels.app", "$.services.spec.selector.app"] },
      { inputs: ["pods", "services"] },
  ]},
  { "@groupBy": ["$.services.metadata.name", "$.pods.status.podIP"] },
  { "@project": {
      kind:     "Endpoints",
      metadata: { name: "$.key" },
      addrs:    "$.values",
  }},
];

// One loop = compiled circuit + set-based plant + delayed echo.
function makeLoop(name, transformName) {
  const out = `endpoints_${name}`;
  const observed = `observed_${name}`;
  const h = aggregate.compile(PIPELINE, {
    inputs: ["pods", "services", observed],
    outputs: [out],
    name: `endpoints-${name}`,
  });
  // The list form applies in canonical order (the loop transform on the
  // snapshot side, then the Incrementalizer) regardless of the order given.
  h.transform([
    { name: "Incrementalizer" },
    transformName === "SmithPredictor"
      ? { name: "SmithPredictor", pairs: [[observed, out]], k: 2 }
      : { name: "Reconciler", pairs: [[observed, out]] },
  ]);
  // No .validate() needed: compile and every transform validate and install
  // the circuit automatically.

  // The plant: a set-based store (idempotent apply = dist); every actual
  // state CHANGE is echoed back after the dead time.
  const store = new Map();
  subscribe(out, (entries) => {
    const echo = [];
    for (const [doc, w] of entries) {
      const key = doc.metadata.name;
      if (w > 0 && !store.has(key)) {
        store.set(key, doc);
        echo.push([doc, 1]);
      } else if (w < 0 && store.has(key)) {
        store.delete(key);
        echo.push([doc, -1]);
      } // duplicate apply: absorbed, no state change, no echo
    }
    if (echo.length > 0) {
      setTimeout(() => publish(observed, echo), DEAD_TIME_MS);
    }
  });
  return { name, out, observed, store };
}

const rec = makeLoop("rec", "Reconciler");
const smith = makeLoop("smith", "SmithPredictor");

// --- The event log: every delivery on every interesting topic is a row. ---
const log = [];
let phase = "";
function fmt(entries) {
  return entries
    .map(([doc, w]) => `${w > 0 ? "+" : w < 0 ? "-" : ""}${doc.metadata.name}[${(doc.addrs || []).length}]`)
    .join(" ");
}
for (const [label, topic] of [
  ["rec out", rec.out],
  ["smith out", smith.out],
  ["rec echo", rec.observed],
  ["smith echo", smith.observed],
]) {
  subscribe(topic, (entries) => {
    if (entries.length > 0) {
      log.push({ phase, label, what: fmt(entries) });
    }
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function run() {
  phase = "preload: 2 pods, no service (no output)";
  publish("pods", [
    [{ metadata: { name: "web-1", labels: { app: "web" } }, status: { podIP: "10.0.0.1" } }, 1],
    [{ metadata: { name: "web-2", labels: { app: "web" } }, status: { podIP: "10.0.0.2" } }, 1],
  ]);
  await sleep(50);

  phase = "t0: Service arrives -> dD = +E";
  publish("services", [[{ metadata: { name: "web-svc" }, spec: { selector: { app: "web" } } }, 1]]);
  await sleep(50); // < dead time: the echo is still in flight

  phase = "t1: unrelated input during dead time (a db pod, no service)";
  publish("pods", [[{ metadata: { name: "db-1", labels: { app: "db" } }, status: { podIP: "10.0.0.9" } }, 1]]);
  await sleep(DEAD_TIME_MS + 100); // echoes land

  phase = "t2..: disturbance W = -E (out-of-band delete from both plants)";
  for (const loop of [rec, smith]) {
    const doc = loop.store.get("web-svc");
    loop.store.delete("web-svc");
    publish(loop.observed, [[doc, -1]]);
  }
  await sleep(DEAD_TIME_MS + 150); // repair + its echo land

  console.log("\nevent log (dead time " + DEAD_TIME_MS + " ms):");
  console.log("| # | phase | signal | entries |");
  console.log("|---+-------+--------+---------|");
  let i = 0;
  let lastPhase = "";
  for (const row of log) {
    const p = row.phase === lastPhase ? "" : row.phase;
    lastPhase = row.phase;
    console.log(`| ${i++} | ${p} | ${row.label} | ${row.what} |`);
  }
  const recEmits = log.filter((r) => r.label === "rec out").length;
  const smithEmits = log.filter((r) => r.label === "smith out").length;
  console.log(`\nemission events: reconciler=${recEmits}, smith=${smithEmits}`);
  console.log(`plant states equal: ${JSON.stringify([...rec.store.keys()]) === JSON.stringify([...smith.store.keys()])}`);
  exit(0);
}

run().catch((e) => { console.log("ERROR", e); exit(1); });
