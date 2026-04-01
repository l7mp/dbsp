runtime.onError((e) => {
  console.error(`[runtime:${e.origin}] ${e.message}`);
});

const minimist = require("minimist");

function parseMode() {
  const parsed = minimist((process?.argv || []).slice(2));
  const mode = String(parsed._?.[0] || "help").toLowerCase();
  if (["help", "test", "controller"].includes(mode)) {
    return mode;
  }
  throw new Error(`invalid mode ${mode}`);
}

function parseSuiteArg() {
  const parsed = minimist((process?.argv || []).slice(2));
  return parsed._?.[1] || parsed.suite || "all";
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
