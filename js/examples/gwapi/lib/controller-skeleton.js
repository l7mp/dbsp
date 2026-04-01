const { CONTROLLER_NAME } = require("./fixtures.js");
const { DEFAULT_TOPICS } = require("./topics.js");

function gatewayClassStatusRecord(doc) {
  return {
    targetRef: {
      apiVersion: "gateway.networking.k8s.io/v1",
      kind: "GatewayClass",
      name: doc?.metadata?.name,
    },
    condition: {
      type: "Accepted",
      status: "True",
      reason: "Accepted",
    },
  };
}

function installGatewayClassStatusController(topics, options = {}) {
  const controllerName = options.controllerName || CONTROLLER_NAME;
  subscribe(topics.inputs.gatewayClass, (entries) => {
    const out = [];
    for (const [doc, weight] of entries) {
      if (doc?.spec?.controllerName !== controllerName) {
        continue;
      }
      out.push([gatewayClassStatusRecord(doc), weight]);
    }
    if (out.length > 0) {
      publish(topics.outputs.gatewayClassStatus, out);
    }
    return entries;
  });
}

function installGatewayStatusControllerPlaceholder(_topics) {}

function installTCPRouteStatusControllerPlaceholder(_topics) {}

function pipelinePlaceholders(topics = DEFAULT_TOPICS, options = {}) {
  const controllerName = options.controllerName || CONTROLLER_NAME;
  return [
    {
      name: "gatewayclass-status",
      inputs: [topics.inputs.gatewayClass],
      outputs: [topics.outputs.gatewayClassStatus],
      aggregationProgram: [
        { "@select": { "@eq": ["$.spec.controllerName", controllerName] } },
        {
          "@project": {
            targetRef: {
              apiVersion: "gateway.networking.k8s.io/v1",
              kind: "GatewayClass",
              name: "$.metadata.name",
            },
            condition: {
              type: "Accepted",
              status: "True",
              reason: "Accepted",
            },
          },
        },
      ],
    },
    {
      name: "gateway-status",
      inputs: [topics.inputs.gatewayClass, topics.inputs.gateway, topics.inputs.secret],
      outputs: [topics.outputs.gatewayStatus],
      aggregationProgram: [],
    },
    {
      name: "tcproute-status",
      inputs: [topics.inputs.gatewayClass, topics.inputs.gateway, topics.inputs.tcpRoute],
      outputs: [topics.outputs.gatewayStatus],
      aggregationProgram: [],
    },
  ];
}

function setupControllerPipelines(options = {}) {
  const topics = options.topics || DEFAULT_TOPICS;
  const controllerName = options.controllerName || CONTROLLER_NAME;

  const controllers = [
    {
      name: "gatewayclass-status",
      install: () => installGatewayClassStatusController(topics, { controllerName }),
    },
    {
      name: "gateway-status",
      install: () => installGatewayStatusControllerPlaceholder(topics),
    },
    {
      name: "tcproute-status",
      install: () => installTCPRouteStatusControllerPlaceholder(topics),
    },
  ];

  for (const c of controllers) {
    c.install();
  }

  return {
    topics,
    controllers,
    placeholders: pipelinePlaceholders(topics, { controllerName }),
  };
}

module.exports = {
  setupControllerPipelines,
  pipelinePlaceholders,
};
