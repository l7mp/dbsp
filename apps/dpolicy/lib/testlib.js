// Test helpers: state-accumulating topic collectors and normalized deep
// comparison. The pipeline emits delta streams (retractions followed by
// re-assertions), so assertions are made against the integrated state of a
// topic, never against individual batches.

const { sleep } = require("testing");

// The operator runtime handle the topic helpers route through: the
// operator's streams live inside its private runtime. Bind it with
// use(handle) before creating collectors or publishing.
let boundHandle = null;

function use(handle) {
  boundHandle = handle;
}

function pub(topic, entries) {
  if (boundHandle) {
    boundHandle.publish(topic, entries);
  } else {
    publish(topic, entries);
  }
}

function sub(topic, cb) {
  if (boundHandle) {
    boundHandle.subscribe(topic, cb);
  } else {
    subscribe(topic, cb);
  }
}

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
  sub(topic, (entries) => {
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

// byName finds the status document of one constraint in a collector state.
function byName(docs, name) {
  return (docs || []).find((d) => d.metadata?.name === name);
}

function upsert(topic, doc) {
  pub(topic, [[doc, 1]]);
}

function retract(topic, doc) {
  pub(topic, [[doc, -1]]);
}

function replace(topic, oldDoc, newDoc) {
  pub(topic, [
    [oldDoc, -1],
    [newDoc, 1],
  ]);
}

module.exports = {
  use,
  collector,
  normalize,
  expectEqual,
  byName,
  upsert,
  retract,
  replace,
};
