// Shared configuration for the delta-gateway operator: controller
// identity, the watched Kubernetes resources, and the topic layout of the
// operator's shared streams.

const CONTROLLER_NAME = "dbsp.l7mp.io/delta-gateway";

const GATEWAY_API_GROUP = "gateway.networking.k8s.io";

// The operator name: the loader prefixes every circuit and shared topic
// with it, and the xDS egress server is registered under it.
const OPERATOR = "delta-gateway";

// The watched Kubernetes resources, as serialized source resources.
const RESOURCES = {
  gatewayClass: { apiGroup: GATEWAY_API_GROUP, version: "v1", kind: "GatewayClass" },
  gateway: { apiGroup: GATEWAY_API_GROUP, version: "v1", kind: "Gateway" },
  httpRoute: { apiGroup: GATEWAY_API_GROUP, version: "v1", kind: "HTTPRoute" },
  backendTLSPolicy: { apiGroup: GATEWAY_API_GROUP, version: "v1", kind: "BackendTLSPolicy" },
  service: { apiGroup: "", version: "v1", kind: "Service" },
  endpointSlice: { apiGroup: "discovery.k8s.io", version: "v1", kind: "EndpointSlice" },
  secret: { apiGroup: "", version: "v1", kind: "Secret" },
  // Namespace labels back the allowedRoutes.namespaces Selector policy.
  namespace: { apiGroup: "", version: "v1", kind: "Namespace" },
};

// The plain-topic binding group: streams of this group are the operator's
// shared retained topics, bound by the runtime's pub/sub itself.
const TOPIC_GROUP = "topic.connector.dcontroller.io";
const XDS_GROUP = "xds.connector.dcontroller.io";

// The loader names an operator's shared topics "<operator>/topic/<stream>".
const topicOf = (stream) => `${OPERATOR}/topic/${stream}`;

// Topic layout of the shared streams, for introspection and the test
// harness. "inputs" carry the watched resources in topic-driven (test)
// mode, "views" the curated views, "observed" the observed statuses the
// Reconciler pairs with the "status" outputs.
const TOPICS = {
  inputs: {
    gatewayClass: topicOf("GatewayClass"),
    gateway: topicOf("Gateway"),
    route: topicOf("HTTPRoute"),
    service: topicOf("Service"),
    endpointSlice: topicOf("EndpointSlice"),
    secret: topicOf("Secret"),
    backendTLSPolicy: topicOf("BackendTLSPolicy"),
    namespace: topicOf("Namespace"),
  },
  views: {
    gatewayClass: topicOf("GatewayClassView"),
    gateway: topicOf("GatewayView"),
    route: topicOf("RouteView"),
    backend: topicOf("BackendView"),
  },
  observed: {
    gatewayClass: topicOf("GatewayClassStatusObserved"),
    gateway: topicOf("GatewayStatusObserved"),
    httpRoute: topicOf("HTTPRouteStatusObserved"),
  },
  status: {
    gatewayClass: topicOf("GatewayClassStatus"),
    gateway: topicOf("GatewayStatus"),
    httpRoute: topicOf("HTTPRouteStatus"),
  },
};

module.exports = {
  CONTROLLER_NAME,
  GATEWAY_API_GROUP,
  OPERATOR,
  RESOURCES,
  TOPIC_GROUP,
  XDS_GROUP,
  TOPICS,
};
