// The endpoints controller under measurement, compiled in one of four
// architectural modes. All modes share the same declarative program; they
// differ only in the transforms applied, which is the point: the benchmark
// compares architectures, not implementations.
//
//   sotw:        Model 1: snapshot adapters on the inputs only (a
//                commit boundary configuration), so the output ships
//                the recomputed full state - the level - every step.
//   sotw-diff:   Model 2: the plain commit (full snapshot adapters);
//                the same recomputed state leaves through the output
//                differentiator as a delta. Same compute, opposite
//                shipping.
//   incremental: automatically incrementalized circuit (Model 3). Both
//                compute and update are delta-sized, but the loop is open.
//   reconciled:  incremental + desired-state Reconciler (Model 4). The
//                output emits the outstanding correction U = I(dD - dY);
//                the "observed" input carries the plant feedback.
//
// The optional indexed flag switches @join from the generic cartesian
// product (predicate evaluated on every pair) to the indexed equi-join.

const OBSERVED = "observed";
// TICK is the dual-rate Smith window clock: the driver pulses it once per
// round in smith mode.
const TICK = "tick";
const OUTPUT = "endpoints";

const JOIN_PRED = {
  "@eq": ["$.pods.metadata.labels.app", "$.services.spec.selector.app"],
};
const JOIN_INDEX = {
  index: { pods: "$.metadata.labels.app", services: "$.spec.selector.app" },
};

// stages returns the per-case pipeline stages (shared by all modes).
function stages(kase, indexed) {
  const join = { "@join": indexed ? [JOIN_PRED, JOIN_INDEX] : [JOIN_PRED] };
  switch (kase) {
    case "join":
      return [
        join,
        {
          "@project": {
            kind: "Endpoint",
            svc: "$.services.metadata.name",
            pod: "$.pods.metadata.name",
            ip: "$.pods.status.podIP",
          },
        },
      ];
    case "pair":
    case "ratio":
    case "fanout":
      return [
        join,
        { "@groupBy": ["$.services.metadata.name", "$.pods.status.podIP"] },
        {
          "@project": {
            kind: "Endpoints",
            metadata: { name: "$.key" },
            endpoints: "$.values",
          },
        },
      ];
    default:
      throw new Error(`unknown case: ${kase}`);
  }
}

// compile builds and validates the circuit for (kase, mode). Returns
// { circuit, output, observed } where observed names the plant-feedback
// topic when the circuit has one and is null otherwise. The closed modes
// read the observed topic; with the watch option every other mode
// subscribes to it too, as an input the program never reads: the
// controller watches the kind it writes, and every watch event triggers a
// step that recomputes the output from the unchanged reference.
function compile(opts) {
  const kase = opts.case;
  const mode = opts.mode;
  const indexed = !!opts.indexed;

  const inputs = ["pods", "services"];
  const closed = mode === "reconciled" || mode === "smith";
  const observed = closed || !!opts.watch;
  if (observed) {
    inputs.push(OBSERVED);
  }

  const program = [
    [{ "@inputs": ["pods", "services"] }, ...stages(kase, indexed), { "@output": OUTPUT }],
  ];
  let c = aggregate.compile(program, {
    inputs: inputs,
    outputs: [{ name: OUTPUT, logical: OUTPUT }],
    name: `endpoints-${kase}-${mode}`,
  });

  switch (mode) {
    case "sotw":
    case "sotw-diff":
      // No transforms: the commit boundary configuration below decides
      // whether the output ships the level (sotw) or its delta.
      break;
    case "incremental":
      c.transform([{ name: "Incrementalizer" }]);
      break;
    case "reconciled":
      c.transform([{ name: "Incrementalizer" }, { name: "Reconciler", pairs: [[OBSERVED, OUTPUT]] }]);
      break;
    case "smith": {
      // The dual-rate Smith: the compensation window is k ticks of the TICK
      // input, which the driver pulses once per round (one empty document
      // asserted per tick, the Tick source's pulse contract), so the window
      // is k rounds against the plant's dead time in rounds.
      const k = Number(opts.smithK);
      if (!Number.isInteger(k) || k < 1) {
        throw new Error(`mode smith needs an integer window smithK >= 1 ticks, got ${opts.smithK}`);
      }
      c.transform([{ name: "Incrementalizer" }, { name: "DualRateSmith", pairs: [[OBSERVED, OUTPUT]], k, tick: TICK }]);
      break;
    }
    default:
      throw new Error(`unknown mode: ${mode}`);
  }
  if (mode === "sotw") {
    // Model 1: adapt the inputs, ship the raw recomputed level.
    c.commit({ inputs: inputs, outputs: [] });
  } else {
    c.commit();
  }
  return { circuit: c, output: OUTPUT, observed: observed ? OBSERVED : null };
}

module.exports = { compile, OUTPUT, OBSERVED, TICK };
