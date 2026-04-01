const t = require("@dbsp/test");
const assert = t.assert;

function sortObject(value) {
  if (Array.isArray(value)) {
    return value.map(sortObject);
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const k of Object.keys(value).sort()) {
      out[k] = sortObject(value[k]);
    }
    return out;
  }
  return value;
}

function normalize(entry) {
  return JSON.stringify(sortObject(entry));
}

function assertMultisetEqual(actualDocs, expectedDocs, label) {
  const count = (items) => {
    const m = new Map();
    for (const it of items) {
      const key = normalize(it);
      m.set(key, (m.get(key) || 0) + 1);
    }
    return m;
  };

  const a = count(actualDocs);
  const e = count(expectedDocs);
  const all = new Set([...a.keys(), ...e.keys()]);
  const diffs = [];
  for (const k of all) {
    const av = a.get(k) || 0;
    const ev = e.get(k) || 0;
    if (av !== ev) {
      diffs.push(`expected=${ev} actual=${av} doc=${k}`);
    }
  }
  if (diffs.length > 0) {
    throw new Error(`${label}: mismatch\n${diffs.join("\n")}`);
  }
}

async function collectTopic(topic, timeoutMs = 50) {
  const iter = subscribe(topic);
  const done = iter.next();
  const timer = new Promise((resolve) => {
    setTimeout(() => resolve({ timeout: true }), timeoutMs);
  });
  const race = await Promise.race([done, timer]);
  await iter.return();

  if (race && race.timeout) {
    return [];
  }
  if (!race || race.done) {
    return [];
  }
  return race.value.map(([doc]) => doc);
}

function runTestCases(cases, runOne) {
  return t.run(cases, runOne);
}

module.exports = {
  assert,
  assertMultisetEqual,
  collectTopic,
  runTestCases,
};
