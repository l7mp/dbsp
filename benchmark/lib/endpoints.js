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
// { circuit, output, observed } where observed is non-null in reconciled
// mode and names the plant-feedback topic.
function compile(opts) {
  const kase = opts.case;
  const mode = opts.mode;
  const indexed = !!opts.indexed;

  const inputs = ["pods", "services"];
  const closed = mode === "reconciled" || mode === "smith";
  if (closed) {
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
      // The SmithPredictor's known dead time in circuit steps: the loop
      // model, distinct from the plant's actual dead time.
      const k = Number(opts.smithK);
      if (!Number.isInteger(k) || k < 2) {
        throw new Error(`mode smith needs an integer dead time smithK >= 2, got ${opts.smithK}`);
      }
      c.transform([{ name: "Incrementalizer" }, { name: "SmithPredictor", pairs: [[OBSERVED, OUTPUT]], k }]);
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
  return { circuit: c, output: OUTPUT, observed: closed ? OBSERVED : null };
}

module.exports = { compile, OUTPUT, OBSERVED };
