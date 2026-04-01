const { CONTROLLER_NAME } = require("./lib/fixtures.js");
const { DEFAULT_TOPICS } = require("./lib/topics.js");
const { setupControllerPipelines } = require("./lib/controller-skeleton.js");

runtime.onError((e) => {
  console.error(`[runtime:${e.origin}] ${e.message}`);
});

function setupConnectors(topics) {
  producer.kubernetes.watch(
    {
      gvk: "gateway.networking.k8s.io/v1/GatewayClass",
      topic: topics.inputs.gatewayClass,
    },
    (entries) => entries,
  );

  producer.kubernetes.watch(
    {
      gvk: "gateway.networking.k8s.io/v1/Gateway",
      topic: topics.inputs.gateway,
    },
    (entries) => entries,
  );

  producer.kubernetes.watch(
    {
      gvk: "v1/Secret",
      topic: topics.inputs.secret,
    },
    (entries) => entries,
  );

  consumer.kubernetes.patcher(
    {
      gvk: "gateway.networking.k8s.io/v1/GatewayClass",
      topic: topics.outputs.gatewayClassStatusPatch,
      subresource: "status",
    },
    (entries) => entries,
  );

  consumer.kubernetes.patcher(
    {
      gvk: "gateway.networking.k8s.io/v1/Gateway",
      topic: topics.outputs.gatewayStatusPatch,
      subresource: "status",
    },
    (entries) => entries,
  );
}

function wireStatusToPatches(topics) {
  subscribe(topics.outputs.gatewayClassStatus, (entries) => {
    publish(topics.outputs.gatewayClassStatusPatch, entries);
    return entries;
  });

  subscribe(topics.outputs.gatewayStatus, (entries) => {
    publish(topics.outputs.gatewayStatusPatch, entries);
    return entries;
  });
}

const topics = DEFAULT_TOPICS;
setupControllerPipelines({ topics, controllerName: CONTROLLER_NAME });
wireStatusToPatches(topics);
setupConnectors(topics);

console.log("GWAPI controller started");
