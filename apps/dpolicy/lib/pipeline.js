// The dpolicy program: two controllers layered around curated views.
//
//   input: watched Gatekeeper policy objects and target resources, folded
//   into the auditor's internal state:
//
//     ConstraintTemplate -+               +-> PolicyView
//     Constraint ---------+  views.js ----+-> TargetView
//     Pod ----------------+               +-> observed statuses (pi_U)
//
//   audit: TargetView x PolicyView -> @rego, rendered into per-constraint
//   status documents (audit.js + status.js) plus the violation/rejection
//   log streams.
//
// The whole operator is one serialized runtime (buildOperatorSpec) fed
// through runtime.create: the circuits communicate over internal streams
// (circuit outputs consumed by circuit inputs, bound to nothing), and
// every stream is a topic of the same name in the operator's private
// runtime, introspectable live with handle.subscribe(). In controller
// mode the status outputs take the desired-state Reconciler against the
// observed constraint statuses; the log streams stay open loop (nothing
// observes a log).

const {
  CONSTRAINT_API_GROUP,
  CONSTRAINT_API_VERSION,
  OPERATOR,
  RESOURCES,
  constraintResource,
  VIOLATION_VIEW_RESOURCE,
  DEFAULT_CONSTRAINT_KINDS,
  VIOLATION_LIMIT,
  topicsFor,
} = require("./config.js");

// Connector-provided operators: LabelSelector matching and Rego policy
// evaluation, both pure Go callbacks; apps opt in per operator.
// Registration is init-phase only and needs no cluster.
kubernetes.expression.register("@selectorMatches", "@rego");
const views = require("./pipeline/views.js");
const audit = require("./pipeline/audit.js");
const status = require("./pipeline/status.js");

// buildPrograms assembles the two layer programs.
function buildPrograms(options = {}) {
  const params = {
    constraintKinds: options.constraintKinds || DEFAULT_CONSTRAINT_KINDS,
    violationLimit: options.violationLimit ?? VIOLATION_LIMIT,
    // The Violation view output: the complete violation set as view
    // objects (one per violation) next to the capped status summary.
    violationViews: options.violationViews ?? false,
  };

  return {
    params,
    input: [...views.branches(params)],
    audit: [...audit.branches(params), ...status.branches(params)],
  };
}

// buildOperatorSpec assembles the serialized runtime. With bindings
// "kubernetes" the sources watch the cluster, the statuses go through
// per-kind Patchers, and the Violation view is served by the view store
// (the deployable spec); with bindings "topics" (the default, the
// self-contained test mode) there are no Kubernetes bindings at all: the
// harness drives the input streams and reads every output through the
// runtime handle.
function buildOperatorSpec(options = {}) {
  const programs = buildPrograms(options);
  const { constraintKinds, violationViews } = programs.params;
  const k8sBindings = options.bindings === "kubernetes";

  const viewStreams = [
    "PolicyView",
    "TargetView",
    ...constraintKinds.map((kind) => `ConstraintStatusObserved_${kind}`),
  ];
  const statusTarget = (kind) => ({
    apiGroup: CONSTRAINT_API_GROUP,
    version: CONSTRAINT_API_VERSION,
    kind,
    type: "Patcher",
    as: `ConstraintStatus_${kind}`,
  });

  // State-of-the-world mode runs the same programs with empty transform
  // chains: the engine commits them with the snapshot adapters
  // (integrators on the inputs, differentiation on the outputs), so the
  // bus still carries deltas and the connectors are the same plain ones.
  // The loop transforms are edge-world constructions and stay off.
  const sotw = Boolean(options.sotw);
  const inputTransforms = sotw ? [] : [{ name: "Incrementalizer" }];
  const auditTransforms = sotw ? [] : [{ name: "Incrementalizer" }];
  if (options.reconcile) {
    // Close the loop on the constraint statuses: the outputs emit the
    // outstanding correction U = ∫(δD − δY_U) - re-emitted every step
    // until the observed state confirms it - so unchanged statuses cost
    // no writes, external tampering heals, and lost writes retry by
    // construction. Controller mode only: the loop needs the watch
    // feedback (the self-contained tests have no plant, so U would never
    // quiesce there).
    auditTransforms.push({
      name: "Reconciler",
      pairs: constraintKinds.map((kind) => [
        `ConstraintStatusObserved_${kind}`,
        `ConstraintStatus_${kind}`,
      ]),
    });
  } else if (options.smith) {
    // The dead-time compensated loop: same closed-loop pairs, but the
    // SmithPredictor emits U_out = (δD − δY_U) + (δY_U ⋉ z⁻¹U) - every
    // correction actuated exactly once, the loop's own watch echoes
    // retired by content, however late the apiserver reflects a write.
    // Controller mode only, like the Reconciler. The dead time k is the
    // loop's known feedback delay in circuit steps and must be given.
    const k = Number(options.smithK);
    if (!Number.isInteger(k) || k < 2) {
      throw new Error(`pipeline: smith mode needs an integer dead time k >= 2, got ${options.smithK}`);
    }
    auditTransforms.push({
      name: "SmithPredictor",
      pairs: constraintKinds.map((kind) => [
        `ConstraintStatusObserved_${kind}`,
        `ConstraintStatus_${kind}`,
      ]),
      k,
    });
  }

  return {
    sources: k8sBindings
      ? [RESOURCES.template, ...constraintKinds.map(constraintResource), RESOURCES.pod]
      : [],
    circuits: [
      {
        name: "input",
        inputs: ["ConstraintTemplate", "Constraint", "Pod"],
        outputs: viewStreams,
        pipeline: programs.input,
        transforms: inputTransforms,
      },
      {
        name: "audit",
        inputs: viewStreams,
        outputs: [
          ...constraintKinds.map((kind) => `ConstraintStatus_${kind}`),
          ...(violationViews ? ["ViolationView"] : []),
          "ViolationLog",
          "RejectionLog",
        ],
        pipeline: programs.audit,
        transforms: auditTransforms,
      },
    ],
    targets: k8sBindings
      ? [
          ...constraintKinds.map(statusTarget),
          ...(violationViews ? [VIOLATION_VIEW_RESOURCE] : []),
        ]
      : [],
  };
}

// Pipeline is the loaded auditor: the serialized runtime fed through
// runtime.create. The circuits are named plainly ("input", "audit") and
// every stream is a topic of the same name in the operator's private
// runtime (this.topics).
class Pipeline {
  constructor(options = {}) {
    this.stem = options.name || OPERATOR;
    this.constraintKinds = options.constraintKinds || DEFAULT_CONSTRAINT_KINDS;
    this.spec = buildOperatorSpec(options);
    this.handle = runtime.create(this.stem, this.spec);
    this.handle.start();
    this.topics = topicsFor(this.constraintKinds);
  }

  // observe attaches a debug observer to one layer ("input" or "audit"):
  // fn receives every event the layer's circuit processes.
  observe(layer, fn) {
    if (!["input", "audit"].includes(layer)) {
      throw new Error(`pipeline.observe: unknown layer ${layer}; use input or audit`);
    }
    this.handle.observe(layer, fn);
  }

  close() {
    this.handle.close();
  }
}

// compilePipeline loads the operator and returns the Pipeline instance.
function compilePipeline(options = {}) {
  return new Pipeline(options);
}

module.exports = {
  Pipeline,
  buildPrograms,
  buildOperatorSpec,
  compilePipeline,
};
