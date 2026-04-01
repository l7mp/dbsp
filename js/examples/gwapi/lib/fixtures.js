const CONTROLLER_NAME = "dbsp.gateway.example/controller";

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function gatewayClassDoc(overrides = {}) {
  return {
    apiVersion: "gateway.networking.k8s.io/v1",
    kind: "GatewayClass",
    metadata: { name: "dbsp-gateway-class" },
    spec: { controllerName: CONTROLLER_NAME },
    ...overrides,
  };
}

function gatewayClassOtherDoc(overrides = {}) {
  return {
    apiVersion: "gateway.networking.k8s.io/v1",
    kind: "GatewayClass",
    metadata: { name: "other-controller-class" },
    spec: { controllerName: "example.net/other-controller" },
    ...overrides,
  };
}

function gatewayDoc(overrides = {}) {
  return {
    apiVersion: "gateway.networking.k8s.io/v1",
    kind: "Gateway",
    metadata: { name: "edge-gw", namespace: "infra" },
    spec: {
      gatewayClassName: "dbsp-gateway-class",
      listeners: [
        {
          name: "https",
          protocol: "HTTPS",
          port: 443,
          hostname: "app.example.com",
          tls: {
            mode: "Terminate",
            certificateRefs: [{ group: "", kind: "Secret", name: "edge-cert" }],
          },
        },
      ],
    },
    ...overrides,
  };
}

function secretDoc(overrides = {}) {
  return {
    apiVersion: "v1",
    kind: "Secret",
    metadata: { name: "edge-cert", namespace: "infra" },
    type: "kubernetes.io/tls",
    data: {
      "tls.crt": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t",
      "tls.key": "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0t",
    },
    ...overrides,
  };
}

function row(doc, weight = 1) {
  return [doc, weight];
}

function rows(docs, weight = 1) {
  return docs.map((d) => row(d, weight));
}

function statusRecord(targetRef, conditionType, conditionStatus, reason) {
  return {
    targetRef,
    condition: {
      type: conditionType,
      status: conditionStatus,
      reason,
    },
  };
}

const sampleResources = {
  gatewayClasses: [gatewayClassDoc(), gatewayClassOtherDoc()],
  gateways: [gatewayDoc()],
  secrets: [secretDoc()],
};

module.exports = {
  CONTROLLER_NAME,
  clone,
  gatewayClassDoc,
  gatewayClassOtherDoc,
  gatewayDoc,
  secretDoc,
  row,
  rows,
  statusRecord,
  sampleResources,
};
