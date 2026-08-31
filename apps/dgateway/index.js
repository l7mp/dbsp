// delta-gateway: a declarative Gateway API operator (HTTP and HTTPS with
// SNI).
//
// Modes:
//   test        run the self-contained test suite (no cluster needed)
//   controller  run against a Kubernetes cluster with the Gateway API CRDs
//   help        print usage
//
// Run from the repo root:
//   ./js/bin/dbsp apps/dgateway/index.js test
//   ./js/bin/dbsp apps/dgateway/index.js controller [--xds-address :18000]

const minimist = require("minimist");

const argv = minimist(process.argv.slice(2));
const mode = String(argv._?.[0] || "help").toLowerCase();

function runHelp() {
  console.log("delta-gateway: declarative Gateway API operator");
  console.log("");
  console.log("Usage:");
  console.log("  ./js/bin/dbsp apps/dgateway/index.js test");
  console.log("  ./js/bin/dbsp apps/dgateway/index.js controller [--xds-address <addr>]");
  console.log("      [--mode reconciler|open|sotw|smith] control-loop architecture (default");
  console.log("                               reconciler: incremental + reconciler; open:");
  console.log("                               incremental without the reconciler; sotw:");
  console.log("                               full recompute; smith: incremental + the");
  console.log("                               Smith dead-time compensator)");
  console.log("      [--debug]                attach layer observers (events on stdout)");
  console.log("      [--address-pool <cidr>]  assign each gateway a loopback data-plane");
  console.log("                               address from this /16 pool (local runs)");
  console.log("      [--qps <n>] [--burst <n>] k8s client budget (default 50/100); low");
  console.log("                               values throttle the controller's writes,");
  console.log("                               stretching the feedback dead time");
  console.log("      [--smith-k <n>]          the Smith predictor's known dead time in");
  console.log("                               circuit steps (smith mode; default 2)");
}

function runController() {
  const { OPERATOR } = require("./lib/config.js");
  const { compilePipeline } = require("./lib/pipeline.js");
  const { createLogger } = require("log");

  const logger = createLogger("delta-gateway");

  const clientOpts = {};
  if (argv.qps !== undefined) {
    clientOpts.qps = Number(argv.qps);
    clientOpts.burst = argv.burst !== undefined ? Number(argv.burst) : 2 * clientOpts.qps;
  } else if (argv.burst !== undefined) {
    clientOpts.burst = Number(argv.burst);
  }
  kubernetes.runtime.start(clientOpts);

  const loopMode = String(argv.mode || "reconciler");
  if (!["reconciler", "open", "sotw", "smith"].includes(loopMode)) {
    throw new Error(`unsupported --mode ${loopMode}; use reconciler, open, sotw or smith`);
  }
  // The xDS egress server carries the operator's name; the loader binds
  // the xDS targets to it.
  const server = xds.server.start({
    name: OPERATOR,
    address: argv["xds-address"] || "127.0.0.1:18000",
  });
  logger.info(`xDS server listening on ${server.address}`);

  const pipeline = compilePipeline({
    bindings: "kubernetes",
    addressPool: argv["address-pool"],
    reconcile: loopMode === "reconciler",
    sotw: loopMode === "sotw",
    smith: loopMode === "smith",
    // The Smith predictor's known dead time, in circuit steps.
    smithK: argv["smith-k"] !== undefined ? Number(argv["smith-k"]) : 2,
  });
  if (argv.debug) {
    // Layer observers: every event each circuit processes, on stdout. The
    // views are ordinary retained topics too - kubectl (through the
    // embedded API server) or subscribe(TOPICS.views.*) inspects the
    // controller's internal state without any of this.
    for (const layer of ["input", "k8s", "xds"]) {
      pipeline.observe(layer, (e) => console.log(`[debug:${layer}]`, JSON.stringify(e)));
    }
  }
  logger.info(`delta-gateway controller started (mode ${loopMode})`);
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
