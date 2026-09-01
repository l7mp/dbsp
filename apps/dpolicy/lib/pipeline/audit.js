// The audit computation: every target is evaluated against every valid
// policy whose match block admits it - the continuous version of the
// Gatekeeper audit scan. The join is what turns the scan inside out: a
// target delta re-evaluates one resource against the (integrated) policy
// state, a policy delta re-evaluates one policy against the target state,
// and nothing at all runs when nothing changes.

const { gkMatches } = require("./expr.js");

function branches() {
  return [
    // One evaluation row per admitted (target, policy) pair. @rego never
    // errors the step: its result document carries the violation set or the
    // failure reason, and the two branches below fan them out.
    [
      { "@inputs": ["TargetView", "PolicyView"] },
      {
        "@join": {
          "@and": [
            { "@isnil": "$.PolicyView.reason" },
            gkMatches("$.PolicyView.match", {
              group: "$.TargetView.gv.group",
              kind: "$.TargetView.kind",
              namespace: "$.TargetView.metadata.namespace",
            }),
            {
              "@selectorMatches": [
                { "@definedOr": ["$.PolicyView.match.labelSelector", null] },
                "$.TargetView.metadata.labels",
              ],
            },
          ],
        },
      },
      {
        "@project": {
          constraint: { kind: "$.PolicyView.kind", name: "$.PolicyView.name" },
          enforcementAction: "$.PolicyView.enforcementAction",
          target: {
            group: "$.TargetView.gv.group",
            version: "$.TargetView.gv.version",
            kind: "$.TargetView.kind",
            name: "$.TargetView.metadata.name",
            namespace: "$.TargetView.metadata.namespace",
          },
          result: {
            "@rego": ["$.PolicyView.rego", "$.PolicyView.parameters", "$.TargetView"],
          },
        },
      },
      { "@output": "evals" },
    ],

    // Violation rows, shaped as Gatekeeper status violation entries next to
    // the owning constraint's identity. Two branches union into the
    // Violations stream: the policy's violation set, and the evaluation
    // failures - an object the policy could not be evaluated against is
    // not known to be compliant, so it surfaces as a violation row too.
    [
      { "@inputs": ["evals"] },
      { "@unwind": "$.result.violations" },
      {
        "@project": {
          constraint: "$.constraint",
          violation: {
            enforcementAction: "$.enforcementAction",
            group: "$.target.group",
            version: "$.target.version",
            kind: "$.target.kind",
            name: "$.target.name",
            namespace: "$.target.namespace",
            message: "$.result.violations.msg",
          },
        },
      },
      { "@output": "Violations" },
    ],
    [
      { "@inputs": ["evals"] },
      { "@select": { "@not": { "@isnil": "$.result.error" } } },
      {
        "@project": {
          constraint: "$.constraint",
          violation: {
            enforcementAction: "$.enforcementAction",
            group: "$.target.group",
            version: "$.target.version",
            kind: "$.target.kind",
            name: "$.target.name",
            namespace: "$.target.namespace",
            message: { "@concat": ["rego evaluation error: ", "$.result.error"] },
          },
        },
      },
      { "@output": "Violations" },
    ],
  ];
}

module.exports = { branches };
