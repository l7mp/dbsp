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
  console.log("      [--mode reconciler|open|smith|sotw]");
  console.log("                                    control-loop architecture (default");
  console.log("                                    reconciler: incremental + reconciler;");
  console.log("                                    open: incremental without the reconciler;");
  console.log("                                    smith: incremental + the dual-rate Smith");
  console.log("                                    compensator; sotw: snapshot execution)");
  console.log("      [--writer absorb|plain]       the actuator: absorb (default) composes");
  console.log("                                    re-emitted changes into one write per");
  console.log("                                    object, plain writes every emitted change");
  console.log("      [--qps <n>] [--burst <n>]     k8s client budget (default 50/100); low");
  console.log("                                    values throttle the controller's writes,");
  console.log("                                    stretching the feedback dead time");
  console.log("      [--smith-window <ms>]         the dual-rate Smith compensation window");
  console.log("                                    in wall time (smith mode; default 10000)");
  console.log("      [--tick-period <ms>]          the window read-out clock's period, the");
  console.log("                                    window's quantum (smith mode; default 1000)");
  console.log("      [--bootstrap auto|manual]     auto (default): the loop is closed from");
  console.log("                                    the first write; manual: the loop runs");
  console.log("                                    open until the operator has been quiet");
  console.log("                                    for --switch-quiet-ms, then closes");
  console.log("      [--switch-quiet-ms <ms>]      the quiet period that ends a manual");
  console.log("                                    bootstrap (default 15000)");
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

  // The client budget (the feedback dead time's lever) and the actuator:
  // absorb (default) composes re-emitted changes into one write per
  // object, plain writes every emitted change as it arrives.
  const clientOpts = {};
  if (argv.qps !== undefined) {
    clientOpts.qps = Number(argv.qps);
    clientOpts.burst = argv.burst !== undefined ? Number(argv.burst) : 2 * clientOpts.qps;
  } else if (argv.burst !== undefined) {
    clientOpts.burst = Number(argv.burst);
  }
  if (argv.writer !== undefined) {
    clientOpts.writer = String(argv.writer);
  }
  kubernetes.runtime.start(clientOpts);

  const loopMode = String(argv.mode || "reconciler");
  if (!["reconciler", "open", "smith", "sotw"].includes(loopMode)) {
    throw new Error(`unsupported --mode ${loopMode}; use reconciler, open, smith or sotw`);
  }
  // The dual-rate Smith window is wall time; the tick period is its
  // quantum. The transform counts ticks, so the window rounds up to
  // whole ticks.
  const tickMs = argv["tick-period"] !== undefined && Number(argv["tick-period"]) > 0
    ? Number(argv["tick-period"]) : 1000;
  const windowMs = argv["smith-window"] !== undefined ? Number(argv["smith-window"]) : 10000;
  if (loopMode === "smith" && !(windowMs > 0)) {
    throw new Error(`--smith-window must be a positive duration in ms, got ${argv["smith-window"]}`);
  }
  // The bootstrap of a closed loop: auto closes it from the first write,
  // manual runs it open until the operator has been quiet for the switch
  // period, then closes it (manual/automatic mode, bumpless).
  const bootstrap = String(argv.bootstrap || "auto");
  if (!["auto", "manual"].includes(bootstrap)) {
    throw new Error(`--bootstrap must be auto or manual, got ${argv.bootstrap}`);
  }
  const quietMs = Number(argv["switch-quiet-ms"] || 15000);
  const closedLoop = loopMode === "reconciler" || loopMode === "smith";
  const pipeline = compilePipeline({
    bindings: "kubernetes",
    bootstrap: closedLoop ? bootstrap : "auto",
    constraintKinds,
    violationLimit: argv["violation-limit"] ? Number(argv["violation-limit"]) : undefined,
    violationViews,
    reconcile: loopMode === "reconciler",
    sotw: loopMode === "sotw",
    smith: loopMode === "smith",
    // The dual-rate Smith compensation window, in ticks of the read-out
    // clock.
    smithK: Math.ceil(windowMs / tickMs),
    tick: `${tickMs}ms`,
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

  if (closedLoop && bootstrap === "manual") {
    pipeline.bootstrapManual(quietMs).then((ms) => {
      logger.info(`bootstrap: loop closed after ${Math.round(ms)}ms (${quietMs}ms of quiet)`);
    });
  }
  logger.info(`dpolicy controller started (mode ${loopMode}${closedLoop ? `, bootstrap ${bootstrap}` : ""}); auditing ${constraintKinds.join(", ")}`);
}

if (mode === "help") {
  runHelp();
  exit();
} else if (mode === "test") {
  require("./tests.js");
} else if (mode === "controller") {
  runController();
} else {
  throw new Error(`unsupported mode ${mode}; use help, test or controller`);
}
