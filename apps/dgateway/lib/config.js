// Shared configuration for the delta-gateway operator: controller identity,
// GVKs of the watched resources, and the pub/sub topic layout.

const CONTROLLER_NAME = "dbsp.l7mp.io/delta-gateway";

const GATEWAY_API_GROUP = "gateway.networking.k8s.io";

const GVKS = {
  gatewayClass: `${GATEWAY_API_GROUP}/v1/GatewayClass`,
  gateway: `${GATEWAY_API_GROUP}/v1/Gateway`,
  httpRoute: `${GATEWAY_API_GROUP}/v1/HTTPRoute`,
  backendTLSPolicy: `${GATEWAY_API_GROUP}/v1/BackendTLSPolicy`,
  service: "v1/Service",
  endpointSlice: "discovery.k8s.io/v1/EndpointSlice",
  secret: "v1/Secret",
  namespace: "v1/Namespace",
};

// Topic layout. "inputs" carry watched Kubernetes resources, "status" carry
// ready-to-write Kubernetes status documents, and "xds" carry the Envoy
// protojson resources consumed by the xDS server (lib/xdsmap).
const TOPICS = {
  inputs: {
    gatewayClass: "delta-gateway.input.gatewayclass",
    gateway: "delta-gateway.input.gateway",
    route: "delta-gateway.input.route",
    service: "delta-gateway.input.service",
    endpointSlice: "delta-gateway.input.endpointslice",
    secret: "delta-gateway.input.secret",
    backendTLSPolicy: "delta-gateway.input.backendtlspolicy",
    // Namespace labels back the allowedRoutes.namespaces Selector policy.
    namespace: "delta-gateway.input.namespace",
  },
  // Curated views: the controller's internal state, one topic per major
  // CRD. The input pipeline folds watched objects into these; the output
  // pipelines render them into statuses and xDS. Views are ordinary
  // (retained) topics, so they are introspectable live.
  views: {
    gatewayClass: "delta-gateway.view.gatewayclass",
    gateway: "delta-gateway.view.gateway",
    route: "delta-gateway.view.route",
    backend: "delta-gateway.view.backend",
  },
  // Observed statuses: the π_U projection of the watched objects into the
  // pure status space the controller owns (compared verbatim: every value the
  // pipeline writes it can regenerate). The k8s output circuit's
  // Reconciler pairs these with the status outputs, so the patcher only
  // ever receives desired-minus-observed differences.
  observed: {
    gatewayClass: "delta-gateway.observed.gatewayclass",
    gateway: "delta-gateway.observed.gateway",
    httpRoute: "delta-gateway.observed.httproute",
  },
  status: {
    gatewayClass: "delta-gateway.status.gatewayclass",
    gateway: "delta-gateway.status.gateway",
    httpRoute: "delta-gateway.status.httproute",
  },
  xds: {
    listeners: "delta-gateway.xds.listeners",
    routes: "delta-gateway.xds.routes",
    clusters: "delta-gateway.xds.clusters",
    endpoints: "delta-gateway.xds.endpoints",
  },
};

module.exports = {
  CONTROLLER_NAME,
  GATEWAY_API_GROUP,
  GVKS,
  TOPICS,
};
