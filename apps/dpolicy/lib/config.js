// Shared configuration for the dpolicy auditor: Gatekeeper API groups, the
// watched resources, and the topic layout of the operator's shared
// streams.
//
// The set of constraint kinds is static configuration: every kind is one
// Gatekeeper-generated CRD under constraints.gatekeeper.sh, watched into
// the shared Constraint stream and given its own status/observed stream
// pair (the status patcher and the Reconciler are per-GVK).

const TEMPLATE_API_GROUP = "templates.gatekeeper.sh";
const CONSTRAINT_API_GROUP = "constraints.gatekeeper.sh";
const CONSTRAINT_API_VERSION = "v1beta1";

// The operator name: the private runtime's name; the Violation view
// group derives from it.
const OPERATOR = "dpolicy";

// The watched Kubernetes resources, as serialized source resources.
const RESOURCES = {
  template: { apiGroup: TEMPLATE_API_GROUP, version: "v1", kind: "ConstraintTemplate" },
  pod: { apiGroup: "", version: "v1", kind: "Pod" },
};

// constraintResource is one Gatekeeper-generated constraint kind; all
// kinds union into the shared Constraint stream (the documents are
// discriminated by their kind field).
function constraintResource(kind) {
  return { apiGroup: CONSTRAINT_API_GROUP, version: CONSTRAINT_API_VERSION, kind, as: "Constraint" };
}

// The Violation view resource: the complete violation set materialized as
// view objects (one per violation) in the operator's own view group - the
// uncapped counterpart of the constraint status summary.
const VIOLATION_VIEW_RESOURCE = {
  apiGroup: `${OPERATOR}.view.dcontroller.io`,
  kind: "Violation",
  as: "ViolationView",
};

const DEFAULT_CONSTRAINT_KINDS = ["K8sRequiredLabels"];

// The Gatekeeper convention: a constraint status reports at most this many
// violation entries next to the full totalViolations count.
const VIOLATION_LIMIT = 20;

// Streams are topics, named plainly: the operator runs in its own
// private runtime, so no prefixing exists.
const topicOf = (stream) => stream;

// topicsFor builds the stream layout for a set of constraint kinds, for
// introspection and the test harness.
function topicsFor(constraintKinds) {
  const observed = {};
  const status = {};
  for (const kind of constraintKinds) {
    observed[kind] = topicOf(`ConstraintStatusObserved_${kind}`);
    status[kind] = topicOf(`ConstraintStatus_${kind}`);
  }
  return {
    inputs: {
      template: topicOf("ConstraintTemplate"),
      // All constraint kinds share one stream; the documents are
      // discriminated by their kind field.
      constraint: topicOf("Constraint"),
      pod: topicOf("Pod"),
    },
    views: {
      policy: topicOf("PolicyView"),
      target: topicOf("TargetView"),
      violation: topicOf("ViolationView"),
    },
    observed,
    status,
    log: {
      violation: topicOf("ViolationLog"),
      rejection: topicOf("RejectionLog"),
    },
  };
}

module.exports = {
  TEMPLATE_API_GROUP,
  CONSTRAINT_API_GROUP,
  CONSTRAINT_API_VERSION,
  OPERATOR,
  RESOURCES,
  constraintResource,
  VIOLATION_VIEW_RESOURCE,
  DEFAULT_CONSTRAINT_KINDS,
  VIOLATION_LIMIT,
  topicsFor,
};
