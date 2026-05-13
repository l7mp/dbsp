const t = require("testing");
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

const topicCollectors = new Map();

function ensureTopicCollector(topic) {
  let collector = topicCollectors.get(topic);
  if (collector) {
    return collector;
  }

  collector = {
    queue: [],
    waiters: [],
  };
  topicCollectors.set(topic, collector);

  subscribe(topic, (entries) => {
    if (collector.waiters.length > 0) {
      const resolve = collector.waiters.shift();
      resolve(entries);
      return;
    }
    collector.queue.push(entries);
  });

  return collector;
}

async function collectTopic(topic, timeoutMs = 50) {
  const collector = ensureTopicCollector(topic);

  if (collector.queue.length > 0) {
    const entries = collector.queue.shift();
    return entries.map(([doc]) => doc);
  }

  const entries = await new Promise((resolve) => {
    let timer = null;
    const onEntries = (batch) => {
      const idx = collector.waiters.indexOf(onEntries);
      if (idx >= 0) {
        collector.waiters.splice(idx, 1);
      }
      clearTimeout(timer);
      resolve(batch);
    };

    collector.waiters.push(onEntries);
    timer = setTimeout(() => {
      const idx = collector.waiters.indexOf(onEntries);
      if (idx >= 0) {
        collector.waiters.splice(idx, 1);
      }
      resolve(null);
    }, timeoutMs);
  });

  if (!entries) {
    return [];
  }
  return entries.map(([doc]) => doc);
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
