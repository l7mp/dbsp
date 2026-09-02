// Noise generators: out-of-band plant disturbances. Each generator mutates
// the plant store directly (the disturbance W lands after the controller's
// write, exactly as in the loop model) and returns the observed-topic
// feedback entries describing the mutation, so a reconciled plant can
// report it the way a watch would.

const { stableStringify } = require("./measure.js");

// corruptDoc returns a structurally valid but wrong copy of a document.
function corruptDoc(doc) {
  const copy = JSON.parse(stableStringify(doc));
  if (Array.isArray(copy.endpoints)) {
    copy.endpoints = ["192.0.2.66", ...copy.endpoints.slice(1)];
  } else if (copy.ip) {
    copy.ip = "192.0.2.66";
  } else {
    copy.corrupted = true;
  }
  return copy;
}

// spuriousDoc fabricates a document that should not exist at all.
function spuriousDoc(i) {
  return {
    kind: "Endpoints",
    metadata: { name: `ghost-${i}` },
    endpoints: ["198.51.100.1"],
  };
}

// inject performs one disturbance of the given kind on the plant and
// reports it on the observed topic (reconciled mode). Kinds:
//
//   delete:  drop a random plant object (a lost or reverted write).
//   corrupt: overwrite a random object with wrong content (an out-of-band
//            edit).
//   insert:  add a spurious object (an unmanaged leftover).
//
// Returns false when the plant has no object to disturb.
let spuriousSeq = 0;

function inject(plant, rng, kind) {
  const keys = [...plant.store.keys()];
  switch (kind) {
    case "delete": {
      if (keys.length === 0) {
        return false;
      }
      const key = keys[Math.floor(rng() * keys.length)];
      const cur = plant.store.get(key);
      plant.store.delete(key);
      plant.report([[cur.doc, -1]]);
      return true;
    }
    case "corrupt": {
      if (keys.length === 0) {
        return false;
      }
      const key = keys[Math.floor(rng() * keys.length)];
      const cur = plant.store.get(key);
      const bad = corruptDoc(cur.doc);
      plant.store.set(key, { doc: bad, json: stableStringify(bad) });
      plant.report([
        [cur.doc, -1],
        [bad, 1],
      ]);
      return true;
    }
    case "insert": {
      const doc = spuriousDoc(++spuriousSeq);
      const json = stableStringify(doc);
      plant.store.set(plant.keyFn(doc), { doc: doc, json: json });
      plant.report([[doc, 1]]);
      return true;
    }
    default:
      throw new Error(`unknown noise kind: ${kind}`);
  }
}

// pick returns a noise kind drawn uniformly from kinds.
function pick(rng, kinds) {
  return kinds[Math.floor(rng() * kinds.length)];
}

module.exports = { inject, pick, corruptDoc, spuriousDoc };
