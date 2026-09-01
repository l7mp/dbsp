// The input layer: watched Gatekeeper policy objects and target resources,
// curated into the auditor's two views, plus the observed-status projection
// backing the Reconciler.
//
//   PolicyView   one document per constraint: the constraint's match and
//                parameters joined with its template's Rego (the template is
//                the only place the policy logic lives - the constraint
//                carries the arguments). Invalid policies stay in the view
//                with a non-null reason, so the status layer can report them
//                and the audit join can skip them.
//
//   TargetView   one document per resource under audit, curated to the
//                fields policies evaluate (identity, labels, annotations,
//                spec) - API-server bookkeeping like resourceVersion and
//                managedFields never enters the circuit, so a no-op update
//                is a no-op delta.

const { matchReason } = require("./expr.js");

function branches({ constraintKinds }) {
  return [
    // Templates: the policy code, keyed by the constraint kind the template
    // generates.
    [
      { "@inputs": ["ConstraintTemplate"] },
      {
        "@project": {
          kind: '$["spec"]["crd"]["spec"]["names"]["kind"]',
          rego: '$["spec"]["targets"][0]["rego"]',
        },
      },
      { "@output": "templates" },
    ],

    // Constraints: the policy arguments, defaulted the Gatekeeper way.
    [
      { "@inputs": ["Constraint"] },
      {
        "@project": {
          kind: "$.kind",
          name: "$.metadata.name",
          match: { "@definedOr": ["$.spec.match", {}] },
          parameters: { "@definedOr": ["$.spec.parameters", {}] },
          enforcementAction: { "@definedOr": ["$.spec.enforcementAction", "deny"] },
        },
      },
      { "@output": "constraints" },
    ],

    // PolicyView: constraint ⋈ template on the constraint kind, validated.
    // The first projection runs the two validators (the match-subset gate
    // and a compile check of the template - @rego without an object only
    // compiles); the second folds them into a single reason field, null for
    // a healthy policy.
    [
      { "@inputs": ["templates", "constraints"] },
      {
        "@join": [
          true,
          {
            index: {
              templates: "$.kind",
              constraints: "$.kind",
            },
          },
        ],
      },
      {
        "@project": {
          kind: "$.constraints.kind",
          name: "$.constraints.name",
          rego: "$.templates.rego",
          match: "$.constraints.match",
          parameters: "$.constraints.parameters",
          enforcementAction: "$.constraints.enforcementAction",
          matchReason: matchReason("$.constraints.match"),
          check: { "@rego": ["$.templates.rego", null, null] },
        },
      },
      {
        "@project": {
          kind: "$.kind",
          name: "$.name",
          rego: "$.rego",
          match: "$.match",
          parameters: "$.parameters",
          enforcementAction: "$.enforcementAction",
          reason: {
            "@cond": [
              { "@isnil": "$.matchReason" },
              {
                "@cond": [
                  { "@isnil": "$.check.error" },
                  null,
                  { "@concat": ["invalid template: ", "$.check.error"] },
                ],
              },
              "$.matchReason",
            ],
          },
        },
      },
      { "@output": "PolicyView" },
    ],

    // TargetView: the resources under audit. Policies see exactly these
    // fields through input.review.object.
    [
      { "@inputs": ["Pod"] },
      {
        "@project": {
          apiVersion: "$.apiVersion",
          kind: "$.kind",
          gvParts: { "@split": ["$.apiVersion", "/"] },
          metadata: {
            name: "$.metadata.name",
            namespace: { "@definedOr": ["$.metadata.namespace", ""] },
            labels: { "@definedOr": ["$.metadata.labels", {}] },
            annotations: { "@definedOr": ["$.metadata.annotations", {}] },
          },
          spec: { "@definedOr": ["$.spec", {}] },
        },
      },
      // Core-group apiVersions have no slash and an empty group; the
      // reshape drops the split scaffold so policies see exactly the
      // curated fields through input.review.object.
      {
        "@project": {
          apiVersion: "$.apiVersion",
          kind: "$.kind",
          gv: {
            group: { "@cond": [{ "@eq": [{ "@len": ["$.gvParts"] }, 2] }, "$.gvParts[0]", ""] },
            version: { "@cond": [{ "@eq": [{ "@len": ["$.gvParts"] }, 2] }, "$.gvParts[1]", "$.apiVersion"] },
          },
          metadata: "$.metadata",
          spec: "$.spec",
        },
      },
      { "@output": "TargetView" },
    ],

    // ------------------------------------------------------ Observed status
    // The π_U projection: the watched constraints' statuses, curated into
    // EXACTLY the shape the audit layer emits (our owned fields only -
    // Gatekeeper components co-own the status object and write byPod, which
    // must stay invisible to the loop). The audit circuit's Reconciler
    // subtracts these from the desired statuses, so unchanged statuses
    // produce no writes and external tampering heals.
    ...constraintKinds.map((kind) => [
      { "@inputs": ["Constraint"] },
      {
        "@select": {
          "@and": [
            { "@eq": ["$.kind", kind] },
            { "@exists": "$.status.totalViolations" },
          ],
        },
      },
      {
        "@project": {
          apiVersion: "$.apiVersion",
          kind: "$.kind",
          metadata: { name: "$.metadata.name" },
          status: {
            totalViolations: "$.status.totalViolations",
            violations: { "@definedOr": ["$.status.violations", []] },
          },
        },
      },
      { "@output": `ConstraintStatusObserved_${kind}` },
    ]),
  ];
}

module.exports = { branches };
