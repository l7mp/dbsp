// Test helpers: state-accumulating topic collectors and normalized deep
// comparison. The pipeline emits delta streams (retractions followed by
// re-assertions), so assertions are made against the integrated state of a
// topic, never against individual batches.

const { sleep } = require("testing");

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

function normalize(value) {
  return JSON.stringify(sortObject(value));
}

// collector integrates a topic: it keeps the net weight per document and
// exposes the documents with positive weight.
function collector(topic) {
  const state = new Map();
  subscribe(topic, (entries) => {
    for (const [doc, weight] of entries) {
      const key = normalize(doc);
      const cur = state.get(key) || { doc, weight: 0 };
      cur.weight += weight;
      if (cur.weight === 0) {
        state.delete(key);
      } else {
        state.set(key, cur);
      }
    }
    return entries;
  });

  return {
    topic,
    docs() {
      return [...state.values()].filter((e) => e.weight > 0).map((e) => e.doc);
    },
    // waitFor polls until pred(docs) returns a truthy value and returns the
    // current docs; throws on timeout with the current state in the message.
    async waitFor(pred, label = "", timeoutMs = 3000) {
      const start = performance.now();
      for (;;) {
        const docs = this.docs();
        if (pred(docs)) {
          return docs;
        }
        if (performance.now() - start > timeoutMs) {
          throw new Error(
            `timeout on ${topic}${label ? ` (${label})` : ""}; state: ${JSON.stringify(docs)}`,
          );
        }
        await sleep(20);
      }
    },
    // waitForQuiet waits until pred holds and keeps holding for settleMs.
    async waitForQuiet(pred, label = "", settleMs = 150, timeoutMs = 3000) {
      await this.waitFor(pred, label, timeoutMs);
      await sleep(settleMs);
      const docs = this.docs();
      if (!pred(docs)) {
        throw new Error(
          `state on ${topic}${label ? ` (${label})` : ""} did not settle: ${JSON.stringify(docs)}`,
        );
      }
      return docs;
    },
  };
}

function expectEqual(actual, expected, label) {
  const a = normalize(actual);
  const e = normalize(expected);
  if (a !== e) {
    throw new Error(`${label}: mismatch\nexpected: ${e}\nactual:   ${a}`);
  }
}

function findCondition(conditions, type) {
  return (conditions || []).find((c) => c.type === type);
}

function expectCondition(conditions, type, status, reason, label) {
  const c = findCondition(conditions, type);
  if (!c) {
    throw new Error(`${label}: condition ${type} not found in ${JSON.stringify(conditions)}`);
  }
  if (c.status !== status || c.reason !== reason) {
    throw new Error(
      `${label}: condition ${type} is ${c.status}/${c.reason}, want ${status}/${reason}`,
    );
  }
  return c;
}

// byName sorts a list of objects with a name field (used to canonicalize
// status.listeners, whose order is deterministic but arbitrary).
function byName(list) {
  return [...(list || [])].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
}

function upsert(topic, doc) {
  publish(topic, [[doc, 1]]);
}

function retract(topic, doc) {
  publish(topic, [[doc, -1]]);
}

function replace(topic, oldDoc, newDoc) {
  publish(topic, [
    [oldDoc, -1],
    [newDoc, 1],
  ]);
}

module.exports = {
  collector,
  normalize,
  expectEqual,
  findCondition,
  expectCondition,
  byName,
  upsert,
  retract,
  replace,
};
