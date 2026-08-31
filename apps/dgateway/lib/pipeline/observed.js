// Observed statuses: the π_U projection of the watched objects' statuses,
// curated into EXACTLY the shape the k8s output layer emits (same fields,
// same order), scoped to the objects this controller owns. Conditions are
// compared verbatim: the pipeline writes only values it holds
// (lastTransitionTime is a @stamp sample held while the condition's status
// lasts), so a status this controller wrote reads back byte-identical. The
// k8s circuit's Reconciler subtracts these from the desired statuses, so
// unchanged statuses produce no writes and external tampering heals.

const { GATEWAY_API_GROUP } = require("../config.js");

function branches({ controllerName, addressPool }) {
  return [
    [
      { "@inputs": ["GatewayClass"] },
      {
        "@select": {
          "@and": [
            { "@eq": ["$.spec.controllerName", controllerName] },
            { "@exists": "$.status.conditions" },
          ],
        },
      },
      {
        "@project": {
          apiVersion: `${GATEWAY_API_GROUP}/v1`,
          kind: "GatewayClass",
          metadata: { name: "$.metadata.name" },
          status: { conditions: "$.status.conditions" },
        },
      },
      { "@output": "GatewayClassStatusObserved" },
    ],
    // Ownership of a Gateway is what the GatewayClassView already
    // established (owned classes only), so the ingest joins on the view.
    [
      { "@inputs": ["GatewayClassView", "Gateway"] },
      {
        "@join": [
          { "@eq": ["$.GatewayClassView.class.name", "$.Gateway.spec.gatewayClassName"] },
          {
            index: {
              GatewayClassView: "$.class.name",
              Gateway: "$.spec.gatewayClassName",
            },
          },
        ],
      },
      { "@select": { "@exists": "$.Gateway.status.conditions" } },
      {
        "@project": {
          apiVersion: `${GATEWAY_API_GROUP}/v1`,
          kind: "Gateway",
          metadata: {
            name: "$.Gateway.metadata.name",
            namespace: "$.Gateway.metadata.namespace",
          },
          status: {
            conditions: "$.Gateway.status.conditions",
            listeners: {
              "@map": [
                {
                  name: "$$.name",
                  supportedKinds: "$$.supportedKinds",
                  attachedRoutes: "$$.attachedRoutes",
                  conditions: "$$.conditions",
                },
                { "@definedOr": ["$.Gateway.status.listeners", []] },
              ],
            },
            ...(addressPool
              ? { addresses: { "@definedOr": ["$.Gateway.status.addresses", []] } }
              : {}),
          },
        },
      },
      { "@output": "GatewayStatusObserved" },
    ],
    [
      { "@inputs": ["HTTPRoute"] },
      {
        "@project": {
          apiVersion: `${GATEWAY_API_GROUP}/v1`,
          kind: "HTTPRoute",
          metadata: { name: "$.metadata.name", namespace: "$.metadata.namespace" },
          status: {
            parents: {
              "@map": [
                {
                  parentRef: "$$.parentRef",
                  controllerName: controllerName,
                  conditions: "$$.conditions",
                },
                {
                  "@filter": [
                    { "@eq": ["$$.controllerName", controllerName] },
                    { "@definedOr": ["$.status.parents", []] },
                  ],
                },
              ],
            },
          },
        },
      },
      { "@select": { "@gt": [{ "@len": ["$.status.parents"] }, 0] } },
      { "@output": "HTTPRouteStatusObserved" },
    ],
  ];
}

module.exports = { branches };
