// The status layer: violation rows folded back into per-constraint status
// documents in the Gatekeeper audit shape (totalViolations plus a capped,
// canonically ordered violations list), one branch per configured constraint
// kind (the status patcher and the Reconciler pair are per-GVK). Rejected
// policies and the raw violation rows also flow out on the log streams.

const { capViolations } = require("./expr.js");
const { CONSTRAINT_API_GROUP, CONSTRAINT_API_VERSION, OPERATOR } = require("../config.js");

const VIOLATION_VIEW_API = `${OPERATOR}.view.dcontroller.io/v1alpha1`;

function branches({ constraintKinds, violationLimit, violationViews }) {
  return [
    // Violations grouped per owning constraint.
    [
      { "@inputs": ["Violations"] },
      { "@groupBy": ["$.constraint", "$.violation"] },
      { "@output": "violationgroups" },
    ],

    // One status document per policy - including the clean ones (a policy
    // with no violations reports totalViolations: 0, the audit's "all
    // clear") and the rejected ones (empty status; the reason goes to the
    // rejection log). The group side is soft: no violations, no partner.
    [
      { "@inputs": ["PolicyView", "violationgroups"] },
      {
        "@join": [
          true,
          {
            index: {
              PolicyView: { kind: "$.kind", name: "$.name" },
              violationgroups: { kind: "$.key.kind", name: "$.key.name" },
            },
            soft: ["violationgroups"],
          },
        ],
      },
      {
        "@project": {
          apiVersion: `${CONSTRAINT_API_GROUP}/${CONSTRAINT_API_VERSION}`,
          kind: "$.PolicyView.kind",
          metadata: { name: "$.PolicyView.name" },
          status: {
            totalViolations: {
              "@len": [{ "@definedOr": ["$.violationgroups.values", []] }],
            },
            violations: capViolations(
              { "@definedOr": ["$.violationgroups.values", []] },
              violationLimit,
            ),
          },
        },
      },
      { "@output": "statusdocs" },
    ],

    // Per-kind status outputs.
    ...constraintKinds.map((kind) => [
      { "@inputs": ["statusdocs"] },
      { "@select": { "@eq": ["$.kind", kind] } },
      { "@output": `ConstraintStatus_${kind}` },
    ]),

    // The Violation view (optional): the complete violation set as one view
    // object per violation, served by the embedded API extension server -
    // the uncapped counterpart of the status summary. Optional because the
    // view store then holds one object per violation, which the operator
    // may not want to pay for on huge violation sets.
    ...(violationViews
      ? [
          [
            { "@inputs": ["Violations"] },
            {
              "@project": {
                apiVersion: VIOLATION_VIEW_API,
                kind: "Violation",
                metadata: {
                  name: {
                    "@concat": [
                      "$.constraint.name",
                      "-",
                      "$.violation.name",
                      "-",
                      { "@hash": "$." },
                    ],
                  },
                  namespace: "$.violation.namespace",
                },
                spec: {
                  constraint: "$.constraint",
                  violation: "$.violation",
                },
              },
            },
            { "@output": "ViolationView" },
          ],
        ]
      : []),

    // Log streams: the violation rows verbatim, and one rejection row per
    // invalid policy.
    [
      { "@inputs": ["Violations"] },
      { "@project": { constraint: "$.constraint", violation: "$.violation" } },
      { "@output": "ViolationLog" },
    ],
    [
      { "@inputs": ["PolicyView"] },
      { "@select": { "@not": { "@isnil": "$.reason" } } },
      { "@project": { constraint: { kind: "$.kind", name: "$.name" }, error: "$.reason" } },
      { "@output": "RejectionLog" },
    ],
  ];
}

module.exports = { branches };
