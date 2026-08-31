// Test fixtures: a GatewayClass/Gateway/HTTPRoute/Service/EndpointSlice
// tuple modeling a web application exposed through an HTTP gateway, plus
// the variants the test cases need.

const { CONTROLLER_NAME, GATEWAY_API_GROUP } = require("./config.js");

function gatewayClass(overrides = {}) {
  return {
    apiVersion: `${GATEWAY_API_GROUP}/v1`,
    kind: "GatewayClass",
    metadata: { name: "delta-gateway" },
    spec: { controllerName: CONTROLLER_NAME },
    ...overrides,
  };
}

function foreignGatewayClass() {
  return gatewayClass({
    metadata: { name: "other-class" },
    spec: { controllerName: "example.net/other-controller" },
  });
}

// The "web" gateway carries a single plain-HTTP listener without a hostname.
function gateway(overrides = {}) {
  return {
    apiVersion: `${GATEWAY_API_GROUP}/v1`,
    kind: "Gateway",
    metadata: { name: "web", namespace: "default" },
    spec: {
      gatewayClassName: "delta-gateway",
      listeners: [{ name: "web", protocol: "HTTP", port: 8080 }],
    },
    ...overrides,
  };
}

function gatewayWithUnsupportedListener() {
  return gateway({
    spec: {
      gatewayClassName: "delta-gateway",
      listeners: [
        { name: "web", protocol: "HTTP", port: 8080 },
        { name: "sctp", protocol: "SCTP", port: 9999 },
      ],
    },
  });
}

// The "secure" gateway carries one HTTPS listener terminating TLS with the
// web-cert Secret.
function secureGateway() {
  return gateway({
    metadata: { name: "secure", namespace: "default" },
    spec: {
      gatewayClassName: "delta-gateway",
      listeners: [
        {
          name: "https",
          protocol: "HTTPS",
          port: 8443,
          tls: { certificateRefs: [{ name: "web-cert" }] },
        },
      ],
    },
  });
}

// tlsSecret is a kubernetes.io/tls Secret; the payloads are base64 strings
// exactly as the API server would deliver them (content is irrelevant to the
// control plane, which passes them through to Envoy inlineBytes).
function tlsSecret() {
  return {
    apiVersion: "v1",
    kind: "Secret",
    metadata: { name: "web-cert", namespace: "default" },
    type: "kubernetes.io/tls",
    data: {
      "tls.crt": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCmNlcnQtY2hhaW4tcGVtCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0=",
      "tls.key": "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCnByaXZhdGUta2V5LXBlbQotLS0tLUVORCBQUklWQVRFIEtFWS0tLS0t",
    },
  };
}

// The "host" gateway carries an HTTP listener restricted to *.example.com.
function hostGateway() {
  return gateway({
    metadata: { name: "host", namespace: "default" },
    spec: {
      gatewayClassName: "delta-gateway",
      listeners: [
        { name: "web", protocol: "HTTP", port: 8081, hostname: "*.example.com" },
      ],
    },
  });
}

// The web route: an exact /login rule to one backend and a /api prefix rule
// split 90/10 between two.
function httpRoute(overrides = {}) {
  return {
    apiVersion: `${GATEWAY_API_GROUP}/v1`,
    kind: "HTTPRoute",
    metadata: { name: "web-route", namespace: "default" },
    spec: {
      parentRefs: [{ name: "web", sectionName: "web" }],
      hostnames: ["www.example.com"],
      rules: [
        {
          matches: [{ path: { type: "Exact", value: "/login" } }],
          backendRefs: [{ name: "login-svc", port: 80 }],
        },
        {
          matches: [{ path: { type: "PathPrefix", value: "/api" } }],
          backendRefs: [
            { name: "api-svc-v1", port: 80, weight: 90 },
            { name: "api-svc-v2", port: 80, weight: 10 },
          ],
        },
      ],
    },
    ...overrides,
  };
}

// An HTTPRoute using an unimplemented filter (RequestMirror): rejected as
// unsupported.
function mirrorRoute() {
  return httpRoute({
    metadata: { name: "mirror-route", namespace: "default" },
    spec: {
      parentRefs: [{ name: "web", sectionName: "web" }],
      rules: [
        {
          matches: [{ path: { type: "PathPrefix", value: "/audit" } }],
          filters: [
            {
              type: "RequestMirror",
              requestMirror: { backendRef: { name: "audit-svc", port: 80 } },
            },
          ],
          backendRefs: [{ name: "login-svc", port: 80 }],
        },
      ],
    },
  });
}

// An HTTPRoute using the implemented (core) filters: a permanent (301)
// hostname redirect on /old, and request header modifiers on /svc.
function filteredRoute() {
  return httpRoute({
    metadata: { name: "filtered-route", namespace: "default" },
    spec: {
      parentRefs: [{ name: "web", sectionName: "web" }],
      hostnames: ["www.example.com"],
      rules: [
        {
          matches: [{ path: { type: "PathPrefix", value: "/old" } }],
          filters: [
            {
              type: "RequestRedirect",
              requestRedirect: { hostname: "new.example.com", statusCode: 301 },
            },
          ],
        },
        {
          matches: [{ path: { type: "PathPrefix", value: "/svc" } }],
          filters: [
            {
              type: "RequestHeaderModifier",
              requestHeaderModifier: {
                set: [{ name: "x-env", value: "prod" }],
                remove: ["x-debug"],
              },
            },
          ],
          backendRefs: [{ name: "login-svc", port: 80 }],
        },
      ],
    },
  });
}

// backendTLSPolicy demands TLS to a backend Service, validated against the
// system CA bundle.
function backendTLSPolicy(serviceName, overrides = {}) {
  return {
    apiVersion: `${GATEWAY_API_GROUP}/v1`,
    kind: "BackendTLSPolicy",
    metadata: { name: `${serviceName}-tls`, namespace: "default" },
    spec: {
      targetRefs: [{ group: "", kind: "Service", name: serviceName }],
      validation: {
        hostname: `${serviceName}.example.internal`,
        wellKnownCACertificates: "System",
      },
    },
    ...overrides,
  };
}

// A policy using caCertificateRefs, which we cannot honor: its clusters are
// suppressed (fail closed).
function unsupportedBackendTLSPolicy(serviceName) {
  return backendTLSPolicy(serviceName, {
    metadata: { name: `${serviceName}-catls`, namespace: "default" },
    spec: {
      targetRefs: [{ group: "", kind: "Service", name: serviceName }],
      validation: {
        hostname: `${serviceName}.example.internal`,
        caCertificateRefs: [{ group: "", kind: "ConfigMap", name: "internal-ca" }],
      },
    },
  });
}

// A web backend: a Service exposing port 80 onto its pods' 8080.
function webService(name) {
  return {
    apiVersion: "v1",
    kind: "Service",
    metadata: { name, namespace: "default" },
    spec: { ports: [{ name: "http", protocol: "TCP", port: 80, targetPort: 8080 }] },
  };
}

function webEndpointSlice(name, addresses) {
  return {
    apiVersion: "discovery.k8s.io/v1",
    kind: "EndpointSlice",
    metadata: {
      name: `${name}-abc12`,
      namespace: "default",
      labels: { "kubernetes.io/service-name": name },
    },
    addressType: "IPv4",
    ports: [{ name: "http", protocol: "TCP", port: 8080 }],
    endpoints: [{ addresses, conditions: { ready: true } }],
  };
}

module.exports = {
  gatewayClass,
  foreignGatewayClass,
  gateway,
  gatewayWithUnsupportedListener,
  secureGateway,
  tlsSecret,
  hostGateway,
  httpRoute,
  mirrorRoute,
  filteredRoute,
  backendTLSPolicy,
  unsupportedBackendTLSPolicy,
  webService,
  webEndpointSlice,
};
