// GatewayClass processing: accept every class we own, curated into the
// GatewayClassView. parametersRef validation is not implemented.
//
// The status writer (statusBranches, k8s output layer) renders the view
// into GatewayClass status documents.

const { GATEWAY_API_GROUP } = require("../config.js");
const { transitionStamp } = require("./expr.js");

function branches({ controllerName }) {
  return [
    [
      { "@inputs": ["GatewayClass"] },
      { "@select": { "@eq": ["$.spec.controllerName", controllerName] } },
      {
        "@project": {
          metadata: { name: "$.metadata.name" },
          class: {
            name: "$.metadata.name",
            generation: { "@definedOr": ["$.metadata.generation", 1] },
          },
          accepted: true,
        },
      },
      { "@output": "GatewayClassView" },
    ],
  ];
}

// ---------------------------------------------------------------------
// GatewayClass status writer: renders GatewayClassView rows into
// GatewayClass status documents. The Accepted transition time is sampled by
// @stamp keyed on the class and its verdict.

function statusBranches() {
  return [
    [
      { "@inputs": ["GatewayClassView"] },
      transitionStamp(["$.class.name", "$.accepted"], ["$.acceptedTime"]),
      {
        "@project": {
          apiVersion: `${GATEWAY_API_GROUP}/v1`,
          kind: "GatewayClass",
          metadata: { name: "$.class.name" },
          status: {
            conditions: [
              {
                type: "Accepted",
                status: "True",
                reason: "Accepted",
                message: "GatewayClass is accepted",
                observedGeneration: "$.class.generation",
                lastTransitionTime: "$.acceptedTime",
              },
            ],
          },
        },
      },
      { "@output": "GatewayClassStatus" },
    ],
  ];
}

module.exports = { branches, statusBranches };
