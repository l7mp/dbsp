const { CONTROLLER_NAME } = require("./lib/fixtures.js");
const { DEFAULT_TOPICS } = require("./lib/topics.js");
const { setupControllerPipelines } = require("./lib/controller-skeleton.js");

runtime.onError((e) => {
  console.error(`[runtime:${e.origin}] ${e.message}`);
});

kubernetes.runtime.start();

function setupConnectors(topics) {
  kubernetes.watch(
    topics.inputs.gatewayClass,
    { gvk: "gateway.networking.k8s.io/v1/GatewayClass" },
    (entries) => entries,
  );

  kubernetes.watch(
    topics.inputs.gateway,
    { gvk: "gateway.networking.k8s.io/v1/Gateway" },
    (entries) => entries,
  );

  kubernetes.watch(
    topics.inputs.secret,
    { gvk: "v1/Secret" },
    (entries) => entries,
  );

  kubernetes.patch(
    topics.outputs.gatewayClassStatusPatch,
    {
      gvk: "gateway.networking.k8s.io/v1/GatewayClass",
      subresource: "status",
    },
    (entries) => entries,
  );

  kubernetes.patch(
    topics.outputs.gatewayStatusPatch,
    {
      gvk: "gateway.networking.k8s.io/v1/Gateway",
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
