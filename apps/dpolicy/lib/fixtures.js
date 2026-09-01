// Kubernetes fixtures for the self-contained test suite: stock Gatekeeper
// policy objects and target pods.

// The stock Gatekeeper required-labels template policy.
const REQUIRED_LABELS_REGO = `
package k8srequiredlabels

violation[{"msg": msg, "details": {"missing_labels": missing}}] {
	provided := {label | input.review.object.metadata.labels[label]}
	required := {label | label := input.parameters.labels[_]}
	missing := required - provided
	count(missing) > 0
	msg := sprintf("you must provide labels: %v", [missing])
}
`;

// The same policy with a distinct message prefix, for template-edit tests.
const REQUIRED_LABELS_REGO_V2 = REQUIRED_LABELS_REGO.replace(
  "you must provide labels",
  "missing mandatory labels",
);

// template builds a ConstraintTemplate for a constraint kind. The
// parameter schema matters when the template feeds a real Gatekeeper: the
// generated constraint CRD is structural, so parameters missing from the
// schema are silently pruned from every constraint.
function template(kind = "K8sRequiredLabels", rego = REQUIRED_LABELS_REGO) {
  return {
    apiVersion: "templates.gatekeeper.sh/v1",
    kind: "ConstraintTemplate",
    metadata: { name: kind.toLowerCase() },
    spec: {
      crd: {
        spec: {
          names: { kind },
          validation: {
            openAPIV3Schema: {
              type: "object",
              properties: {
                labels: { type: "array", items: { type: "string" } },
              },
            },
          },
        },
      },
      targets: [{ target: "admission.k8s.gatekeeper.sh", rego }],
    },
  };
}

// constraint builds a K8sRequiredLabels constraint requiring the given
// labels on pods, with an optional match block.
function constraint(name, { kind = "K8sRequiredLabels", labels = ["owner"], match } = {}) {
  return {
    apiVersion: "constraints.gatekeeper.sh/v1beta1",
    kind,
    metadata: { name },
    spec: {
      ...(match ? { match } : {}),
      parameters: { labels },
    },
  };
}

// pod builds a minimal pod under audit.
function pod(name, { namespace = "default", labels } = {}) {
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      name,
      namespace,
      ...(labels ? { labels } : {}),
    },
    spec: { containers: [{ name: "app", image: "example.com/app:1" }] },
  };
}

module.exports = {
  REQUIRED_LABELS_REGO,
  REQUIRED_LABELS_REGO_V2,
  template,
  constraint,
  pod,
};
