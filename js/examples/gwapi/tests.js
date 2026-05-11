const { rows } = require("./lib/fixtures.js");
const { DEFAULT_TOPICS, withPrefix } = require("./lib/topics.js");
const { setupControllerPipelines } = require("./lib/controller-skeleton.js");
const { gwclassCases } = require("./lib/test-cases-gwclass.js");
const { assertMultisetEqual, collectTopic, runTestCases } = require("./lib/test-runtime.js");

function pickSuite(arg) {
  if (!arg || arg === "all") {
    return "all";
  }
  return arg;
}

function parseSuiteArg(argv) {
  if (Array.isArray(argv) && argv.length >= 1) {
    return argv[0];
  }
  return "all";
}

async function runSuite(suiteName) {
  const topics = withPrefix(`test.${suiteName}`, DEFAULT_TOPICS);
  setupControllerPipelines({ topics });

  const suites = {
    gwclass: gwclassCases(topics),
  };

  let cases = [];
  if (suiteName === "all") {
    for (const list of Object.values(suites)) {
      cases = cases.concat(list);
    }
  } else if (suites[suiteName]) {
    cases = suites[suiteName];
  } else {
    throw new Error(`unknown suite ${suiteName}; available: all, ${Object.keys(suites).join(", ")}`);
  }

  const runOne = async (tc) => {
    for (const [topic, docs] of Object.entries(tc.inputs || {})) {
      publish(topic, rows(docs));
    }
    for (const [topic, expectedDocs] of Object.entries(tc.expected || {})) {
      const actualDocs = await collectTopic(topic, 80);
      assertMultisetEqual(actualDocs, expectedDocs || [], `${tc.name} ${topic}`);
    }
  };

  await runTestCases(cases, runOne);
}

async function runSelectedSuite(argv = []) {
  const suite = pickSuite(parseSuiteArg(argv));
  await runSuite(suite);
}

module.exports = {
  runSuite,
  runSelectedSuite,
};
