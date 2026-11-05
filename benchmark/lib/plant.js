// A minimal in-process plant: the keyed store the controller drives. It
// models exactly what the DBSP control-loop theory assumes the plant to be
// (z^-1 ∘ I: an integrator holding the last written state) while staying
// observable from JS with zero measurement delay.
//
// The apply semantics per architectural mode:
//
//   sotw:        the step carries the full desired snapshot; the plant is
//                overwritten wholesale (level-triggered apply).
//   sotw-diff:   the step carries the full snapshot; the plant applier
//                diffs it against the previous snapshot and applies only
//                the change (this is where Model 2's O(N) diff runs).
//   incremental: the step carries a delta; the plant integrates it
//                blindly. Nothing ever re-reads the plant: open loop.
//   reconciled:  the step carries the outstanding correction U; the plant
//                applies it idempotently and reports its own change back
//                on the observed topic - the watch feedback closing the
//                loop.
//
// Out-of-band tampering (the noise generators) mutates the store directly;
// when the circuit has an observed topic the mutation is also reported on
// it, exactly as a Kubernetes watch would report a kubectl edit. With the
// echo option the snapshot and delta plants report their own writes too
// (the reconciled plant always does): the watch of a controller that
// watches the kind it writes.

const { stableStringify } = require("./measure.js");

class Plant {
  constructor(opts) {
    this.mode = opts.mode;
    this.keyFn = opts.keyFn;
    this.observed = opts.observed || null;
    this.store = new Map(); // key -> { doc, json }
    this.writes = 0; // objects written (incl. deletes), one per key and apply
    this.writeBytes = 0; // serialized bytes written
    this.lastFeedback = 0; // entries fed back on the last step
    this.echo = !!opts.echo;
    // touched maps every key written in the current apply to its entry
    // before the apply (null for an absent key).
    this.touched = null;
    this.applied = 0; // entries RECEIVED by the applier (actuation volume)
    // Feedback dead time, in ticks: observed-topic publications are held
    // back deadTime tick() calls (0 = immediate, the classical one-step
    // feedback model). The driver clocks tick() once per round.
    this.deadTime = opts.deadTime || 0;
    this.clock = 0;
    this.fbQueue = [];
    // additive mode: a weight-integrating store keyed by content (the
    // Z-set integrator with NO dist clamp - the plant the model warns
    // about).
    this.aw = new Map(); // json -> { doc, w }
    // Write-loss injection: each correction entry is dropped with
    // probability dropP BEFORE it reaches the store - the write was
    // actuated but never landed, so there is no state change and no echo.
    // dropRng is the seeded uniform source; dropped counts the losses.
    this.dropP = opts.dropP || 0;
    this.dropRng = opts.dropRng || Math.random;
    this.dropped = 0;
  }

  // connect installs the plant as the collector's apply hook.
  connect(collector) {
    collector.apply = (entries) => this.applyStep(entries);
  }

  applyStep(entries) {
    this.applied += entries.length;
    this.touched = new Map();
    switch (this.mode) {
      case "sotw":
        this.applySnapshot(entries);
        break;
      case "sotw-diff":
        this.applySnapshotDiff(entries);
        break;
      case "incremental":
        this.applyDelta(entries);
        break;
      case "reconciled":
      case "smith":
      case "idempotent":
        this.applyCorrection(entries);
        break;
      case "additive":
        this.applyAdditive(entries);
        break;
      default:
        throw new Error(`unknown plant mode: ${this.mode}`);
    }
    this.writes += this.touched.size;
    if (this.echo && (this.mode === "sotw" || this.mode === "sotw-diff" || this.mode === "incremental")) {
      const fb = [];
      for (const [key, prior] of this.touched) {
        const cur = this.store.get(key);
        if (prior && (!cur || cur.json !== prior.json)) {
          fb.push([prior.doc, -1]);
        }
        if (cur && (!prior || prior.json !== cur.json)) {
          fb.push([cur.doc, 1]);
        }
      }
      this.publishFb(fb);
    }
    this.touched = null;
  }

  // publishFb sends feedback on the observed topic, delayed by the
  // configured dead time (in driver ticks).
  publishFb(fb) {
    if (!this.observed || fb.length === 0) {
      return;
    }
    if (this.deadTime === 0) {
      publish(this.observed, fb);
      return;
    }
    this.fbQueue.push({ due: this.clock + this.deadTime, fb: fb });
  }

  // tick advances the feedback clock and releases the deliveries whose
  // dead time expired. The driver calls it once per round.
  tick() {
    this.clock++;
    while (this.fbQueue.length > 0 && this.fbQueue[0].due <= this.clock) {
      publish(this.observed, this.fbQueue.shift().fb);
    }
  }

  // applyAdditive integrates the received delta by content WITHOUT the
  // set clamp: weights accumulate, so a re-applied correction
  // double-counts - the additive plant of the model, deployed. The plant
  // echoes what it absorbed (its state delta equals the applied delta).
  applyAdditive(entries) {
    for (const [doc, w] of entries) {
      const json = stableStringify(doc);
      const cur = this.aw.get(json);
      const next = (cur ? cur.w : 0) + w;
      if (next === 0) {
        this.aw.delete(json);
      } else {
        this.aw.set(json, { doc: doc, w: next });
      }
      this.writes++;
      this.writeBytes += json.length;
    }
    this.lastFeedback = entries.length;
    this.publishFb(entries.map(([d, w]) => [d, w]));
  }

  // additiveDrift measures the additive store against the oracle: the sum
  // of |held weight - desired weight| over all contents.
  additiveDrift(oracle) {
    const desired = new Set(oracle.values());
    let d = 0;
    for (const json of desired) {
      const cur = this.aw.get(json);
      d += Math.abs((cur ? cur.w : 0) - 1);
    }
    for (const [json, rec] of this.aw) {
      if (!desired.has(json)) {
        d += Math.abs(rec.w);
      }
    }
    return d;
  }

  // set and del write one object; the apply counts the distinct keys it
  // touched, so an update that arrives as a retraction and an insertion is
  // one write.
  set(key, doc, json) {
    this.touch(key);
    this.store.set(key, { doc: doc, json: json });
    this.writeBytes += json.length;
  }

  del(key) {
    this.touch(key);
    this.store.delete(key);
  }

  touch(key) {
    if (this.touched && !this.touched.has(key)) {
      this.touched.set(key, this.store.get(key) || null);
    }
  }

  // applySnapshot replaces the whole plant with the received snapshot: every
  // object of the snapshot is written and every other object is deleted.
  applySnapshot(entries) {
    if (entries.length === 0) {
      return; // A quiescent step is not a resync.
    }
    const next = new Map();
    for (const [doc, w] of entries) {
      if (w > 0) {
        next.set(this.keyFn(doc), doc);
      }
    }
    for (const key of [...this.store.keys()]) {
      if (!next.has(key)) {
        this.del(key);
      }
    }
    for (const [key, doc] of next) {
      this.set(key, doc, stableStringify(doc));
    }
  }

  // applySnapshotDiff applies only the difference against the current
  // plant content (Model 2: full recompute, delta-sized writes).
  applySnapshotDiff(entries) {
    if (entries.length === 0) {
      return;
    }
    const next = new Map();
    for (const [doc, w] of entries) {
      if (w > 0) {
        next.set(this.keyFn(doc), doc);
      }
    }
    for (const key of [...this.store.keys()]) {
      if (!next.has(key)) {
        this.del(key);
      }
    }
    for (const [key, doc] of next) {
      const json = stableStringify(doc);
      const cur = this.store.get(key);
      if (!cur || cur.json !== json) {
        this.set(key, doc, json);
      }
    }
  }

  // applyDelta integrates the received delta. Retractions are
  // content-checked (delete only what the retraction names), like a real
  // edge-triggered applier using precondition-guarded deletes; without the
  // check, the arbitrary entry order within a step would let a retraction
  // clobber an insert of the same key. The check needs no plant read-back  - 
  // it compares against the applier's own last write.
  applyDelta(entries) {
    const dels = [];
    for (const [doc, w] of entries) {
      if (w < 0) {
        dels.push(doc);
      }
    }
    for (const doc of dels) {
      const key = this.keyFn(doc);
      const cur = this.store.get(key);
      if (cur && cur.json === stableStringify(doc)) {
        this.del(key);
      }
    }
    for (const [doc, w] of entries) {
      if (w > 0) {
        this.set(this.keyFn(doc), doc, stableStringify(doc));
      }
    }
  }

  // applyCorrection applies the outstanding correction idempotently and
  // publishes the plant's own change to the observed topic.
  applyCorrection(entries) {
    const fb = [];
    for (const [doc, w] of entries) {
      if (this.dropP > 0 && this.dropRng() < this.dropP) {
        this.dropped++;
        continue;
      }
      const key = this.keyFn(doc);
      const json = stableStringify(doc);
      const cur = this.store.get(key);
      if (w > 0) {
        if (!cur || cur.json !== json) {
          if (cur) {
            fb.push([cur.doc, -1]);
          }
          this.set(key, doc, json);
          fb.push([doc, 1]);
        }
      } else if (w < 0) {
        if (cur && cur.json === json) {
          this.del(key);
          fb.push([doc, -1]);
        }
      }
    }
    this.lastFeedback = fb.length;
    this.publishFb(fb);
  }

  // report publishes an out-of-band store mutation on the observed topic,
  // if the circuit has one: the watch reporting external tampering.
  report(fb) {
    this.publishFb(fb);
  }

  // quiesce settles the reconciliation loop. In reconciled mode the plant
  // applies the whole outstanding correction U in place on the visible
  // correction step (the apply hook, which runs before that step's waiter
  // resolves), so the plant already holds the desired state by the time the
  // caller regains control. Convergence is single-round: the desired state
  // does not move while the plant corrects (dD = 0), so U falls to zero on
  // the next feedback round. That final round carries an empty delta, which
  // the runtime suppresses before it reaches the output topic, so it is not
  // observable as a step - there is nothing to wait for. The feedback the
  // plant published to zero the circuit's accumulator is FIFO-ordered ahead
  // of any later input, so the accumulator is consistent by the time the
  // next output is produced. Other modes settle in zero steps too.
  //
  // The parameters are retained so every await site reads uniformly across
  // modes.
  async quiesce(_collector, _timeoutMs) {}

  // drift returns the number of wrong objects: the keys whose plant content
  // differs from the oracle desired state, missing, corrupted and spurious
  // alike.
  drift(oracle) {
    let d = 0;
    for (const [key, json] of oracle) {
      const cur = this.store.get(key);
      if (!cur || cur.json !== json) {
        d += 1;
      }
    }
    for (const key of this.store.keys()) {
      if (!oracle.has(key)) {
        d += 1;
      }
    }
    return d;
  }
}

module.exports = { Plant };
