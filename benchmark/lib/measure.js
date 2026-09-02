// Measurement apparatus: a step collector on the output topic, summary
// statistics, and CSV output.

const fs = require("fs");

// Collector subscribes to an output topic and tracks the steps delivered on
// it. The runtime suppresses empty output deltas and folds queued input into
// one step, so an input publish does not map one-to-one to a delivered step:
// a publish that produces no output change delivers nothing. Synchronization
// therefore keys on output content (waitEntry) rather than step counts;
// waitSteps remains for cases whose step is known to be non-empty.
//
// An optional apply(entries) hook (the E2 plant, the reconciled-mode
// feedback loop) runs before waiters are checked, so a waiter that resolves
// on an output entry observes the plant already updated.
class Collector {
  constructor(topic) {
    this.steps = 0;
    this.entries = 0;
    this.bytes = 0;
    this.apply = null;
    this.waiters = [];
    subscribe(topic, (entries) => this.onStep(entries));
  }

  onStep(entries) {
    this.steps++;
    this.entries += entries.length;
    for (const [doc, _w] of entries) {
      this.bytes += JSON.stringify(doc).length;
    }
    if (this.apply) {
      this.apply(entries);
    }
    const pending = this.waiters;
    this.waiters = [];
    for (const w of pending) {
      if (w.pred(entries)) {
        w.resolve();
      } else {
        this.waiters.push(w);
      }
    }
  }

  // mark returns a snapshot of the counters, for windowed measurements.
  mark() {
    return { steps: this.steps, entries: this.entries, bytes: this.bytes };
  }

  // since returns counter deltas relative to a mark().
  since(m) {
    return {
      steps: this.steps - m.steps,
      entries: this.entries - m.entries,
      bytes: this.bytes - m.bytes,
    };
  }

  // waitFor resolves when a step arrives for which pred(entries) is true.
  waitFor(pred, timeoutMs, label) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(
        () => reject(new Error(`timeout waiting for ${label || "step"} after ${timeoutMs}ms`)),
        timeoutMs,
      );
      this.waiters.push({
        pred: pred,
        resolve: () => {
          clearTimeout(t);
          resolve();
        },
      });
    });
  }

  // waitSteps resolves after k further steps.
  waitSteps(k, timeoutMs, label) {
    const target = this.steps + k;
    return this.waitFor(() => this.steps >= target, timeoutMs, label);
  }

  // waitEntry resolves when an output entry with the given weight sign
  // matches pred(doc).
  waitEntry(pred, sign, timeoutMs, label) {
    return this.waitFor(
      (entries) => entries.some(([d, w]) => Math.sign(w) === sign && pred(d)),
      timeoutMs,
      label,
    );
  }
}

function quantile(sorted, q) {
  if (sorted.length === 0) {
    return NaN;
  }
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

// stats returns { n, mean, median, p95, min, max } of a sample.
function stats(xs) {
  const s = xs.slice().sort((a, b) => a - b);
  const mean = s.reduce((a, b) => a + b, 0) / (s.length || 1);
  return {
    n: s.length,
    mean: mean,
    median: quantile(s, 0.5),
    p95: quantile(s, 0.95),
    min: s[0],
    max: s[s.length - 1],
  };
}

// csvAppend appends one row to path, writing the header first when the
// file does not exist yet.
function csvAppend(path, header, row) {
  if (!fs.existsSync(path)) {
    fs.writeFileSync(path, header.join(",") + "\n");
  }
  fs.appendFileSync(path, row.join(",") + "\n");
}

// stableStringify serializes with sorted object keys. Documents round-trip
// through Go maps, which do not preserve key order, so content comparison
// must not depend on it.
function stableStringify(v) {
  if (v === null || typeof v !== "object") {
    return JSON.stringify(v);
  }
  if (Array.isArray(v)) {
    return "[" + v.map(stableStringify).join(",") + "]";
  }
  const keys = Object.keys(v).sort();
  return "{" + keys.map((k) => JSON.stringify(k) + ":" + stableStringify(v[k])).join(",") + "}";
}

// mulberry32 is a tiny seeded PRNG, enough for reproducible noise.
function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

module.exports = { Collector, stats, quantile, csvAppend, mulberry32, stableStringify };
