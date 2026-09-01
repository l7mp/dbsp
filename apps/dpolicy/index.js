// dpolicy: a continuous audit controller for Gatekeeper policies.
//
// Modes:
//   test        run the self-contained test suite (no cluster needed)
//   controller  run against a Kubernetes cluster with Gatekeeper CRDs
//   help        print usage
//
// Run from the repo root:
//   ./js/bin/dbsp apps/dpolicy/index.js test
//   ./js/bin/dbsp apps/dpolicy/index.js controller [--constraint-kinds <k1,k2>]

const minimist = require("minimist");

const argv = minimist(process.argv.slice(2));
const mode = String(argv._?.[0] || "help").toLowerCase();

function runHelp() {
  console.log("dpolicy: continuous audit controller for Gatekeeper policies");
  console.log("");
  console.log("Usage:");
  console.log("  ./js/bin/dbsp apps/dpolicy/index.js test");
  console.log("  ./js/bin/dbsp apps/dpolicy/index.js controller");
  console.log("      [--constraint-kinds <kinds>]  comma-separated constraint kinds to");
  console.log("                                    audit (default: K8sRequiredLabels)");
  console.log("      [--violation-limit <n>]       violation entries kept per status (20)");
  console.log("      [--no-violation-views]        drop the Violation view objects (the");
  console.log("                                    complete set served by the extension");
  console.log("                                    API server; one object per violation)");
  console.log("      [--mode reconciler|open|smith|sotw] control-loop architecture");
  console.log("                                    (default reconciler: incremental +");
  console.log("                                    reconciler; open: incremental without");
  console.log("                                    the reconciler; smith: incremental +");
  console.log("                                    the Smith dead-time compensator;");
  console.log("                                    sotw: snapshot execution)");
  console.log("      [--smith-k <n>]               the Smith predictor's known dead time");
  console.log("                                    in circuit steps (smith mode; default 2)");
  console.log("      [--debug]                     attach layer observers (events on stdout)");
}

function runController() {
  const { DEFAULT_CONSTRAINT_KINDS } = require("./lib/config.js");
  const { compilePipeline } = require("./lib/pipeline.js");
  const { createLogger } = require("log");

  const logger = createLogger("dpolicy");

  const constraintKinds = argv["constraint-kinds"]
    ? String(argv["constraint-kinds"]).split(",").map((k) => k.trim()).filter(Boolean)
    : DEFAULT_CONSTRAINT_KINDS;
  // --no-violation-views drops the Violation view output (and its one
  // view object per violation).
  const violationViews = argv["violation-views"] !== false;

  kubernetes.runtime.start();

  const loopMode = String(argv.mode || "reconciler");
  if (!["reconciler", "open", "smith", "sotw"].includes(loopMode)) {
    throw new Error(`unsupported --mode ${loopMode}; use reconciler, open, smith or sotw`);
  }
  const pipeline = compilePipeline({
    bindings: "kubernetes",
    constraintKinds,
    violationLimit: argv["violation-limit"] ? Number(argv["violation-limit"]) : undefined,
    violationViews,
    reconcile: loopMode === "reconciler",
    sotw: loopMode === "sotw",
    smith: loopMode === "smith",
    // The Smith predictor's known dead time, in circuit steps.
    smithK: argv["smith-k"] !== undefined ? Number(argv["smith-k"]) : 2,
  });
  if (argv.debug) {
    for (const layer of ["input", "audit"]) {
      pipeline.observe(layer, (e) => console.log(`[debug:${layer}]`, JSON.stringify(e)));
    }
  }
  const topics = pipeline.topics;

  // The audit log: every violation raised or cleared, and every rejected
  // policy, as JSONL on stdout.
  pipeline.handle.subscribe(topics.log.violation, (entries) => {
    for (const [doc, weight] of entries) {
      console.log(JSON.stringify({ event: weight > 0 ? "violation" : "resolved", ...doc }));
    }
    return entries;
  });
  pipeline.handle.subscribe(topics.log.rejection, (entries) => {
    for (const [doc, weight] of entries) {
      console.log(JSON.stringify({ event: weight > 0 ? "rejected" : "unrejected", ...doc }));
    }
    return entries;
  });

  logger.info(`dpolicy controller started (mode ${loopMode}); auditing ${constraintKinds.join(", ")}`);
}

if (mode === "help") {
  runHelp();
} else if (mode === "test") {
  require("./tests.js");
} else if (mode === "controller") {
  runController();
} else {
  throw new Error(`unsupported mode ${mode}; use help, test or controller`);
}
