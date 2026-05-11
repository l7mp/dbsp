const minimist = require("minimist");

const argv = minimist(process.argv.slice(2));

function parseMode() {
  const mode = String(argv._?.[0] || "help").toLowerCase();
  if (["help", "test", "controller"].includes(mode)) {
    return mode;
  }
  throw new Error(`invalid mode ${mode}`);
}

function parseSuiteArg() {
  return argv._?.[1] || argv.suite || "all";
}

function runHelp() {
  console.log("Gateway API controller examples (js/examples/gwapi)");
  console.log("");
  console.log("Run from js/:");
  console.log("  ./bin/dbsp examples/gwapi/index.js help");
  console.log("  ./bin/dbsp examples/gwapi/index.js test gwclass");
  console.log("  ./bin/dbsp examples/gwapi/index.js test all");
  console.log("  ./bin/dbsp examples/gwapi/index.js controller");
  console.log("");
  console.log("Modes:");
  console.log("- help: prints this message");
  console.log("- test: runs fixture-based tests (suite arg: all|gwclass)");
  console.log("- controller: runs controller connectors and pipelines");
}

const mode = parseMode();
if (mode === "help") {
  runHelp();
} else if (mode === "test") {
  await require("./tests.js").runSelectedSuite([parseSuiteArg()]);
} else if (mode === "controller") {
  require("./controller.js");
} else {
  throw new Error(`unsupported mode ${mode}`);
}
