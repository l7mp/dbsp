"use strict";

const { describe } = require("testing");
const { DControllerManager } = require("../../lib/manager");
const { RuntimeConfig } = require("../../lib/config");
const { createLogger } = require("log");

const OPERATOR_GVK = "dcontroller.io/v1alpha1/Operator";
const SVC_GVK      = "v1/Service";
const ES_GVK       = "discovery.k8s.io/v1/EndpointSlice";
const TESTNS       = "testnamespace";
const CTRL_ANN     = "dcontroller.io/endpointslice-controller-enabled";
const OP_NAME      = "ep-gather-op";
const EP_TOPIC     = `${OP_NAME}.endpointslice-controller/EndpointView/output`;

const config = new RuntimeConfig();
const runtimeConfig = config.makeFromEnv();
const manager = new DControllerManager({
    runtimeConfig,
    logger: createLogger("test-manager"),
});
manager.start();

const writeOperator = kubernetes.update("write-operator", { gvk: OPERATOR_GVK });
const writeSvc      = kubernetes.update("write-svc",      { gvk: SVC_GVK });
const writeES       = kubernetes.update("write-es",       { gvk: ES_GVK });

kubernetes.watch("watch-op", { gvk: OPERATOR_GVK });

// --- Operator-status checker -----------------------------------------------

const opCheckers = [];
subscribe("watch-op", (entries) => {
    for (const fn of opCheckers.slice()) fn(entries);
});

function waitForOpReady(opName, timeoutMs = 10000) {
    return new Promise((resolve, reject) => {
        let done = false;
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            const i = opCheckers.indexOf(checker);
            if (i >= 0) opCheckers.splice(i, 1);
            reject(new Error(`timeout (${timeoutMs}ms) waiting for operator ${opName} ready`));
        }, timeoutMs);
        function checker(entries) {
            if (done) return;
            for (const [obj, w] of entries) {
                if (w <= 0 || obj.metadata.name !== opName) continue;
                const cond = (obj.status?.conditions || []).find(c => c.type === "Ready");
                if (cond?.status === "True" && cond?.reason === "Ready") {
                    done = true;
                    clearTimeout(timer);
                    const i = opCheckers.indexOf(checker);
                    if (i >= 0) opCheckers.splice(i, 1);
                    resolve(obj);
                    return;
                }
            }
        }
        opCheckers.push(checker);
    });
}

// --- EndpointView subscriber -----------------------------------------------

const endpointSpecCounts = new Map(); // JSON(spec) -> { spec, count }
const evCheckers = [];

function specKey(spec) {
    return JSON.stringify(spec || {});
}

function applyEndpointSpec(spec, w) {
    if (!spec) return;
    const key = specKey(spec);
    const current = endpointSpecCounts.get(key);
    const next = (current?.count || 0) + w;
    if (next <= 0) {
        endpointSpecCounts.delete(key);
        return;
    }
    endpointSpecCounts.set(key, { spec, count: next });
}

function currentEndpointSpecs() {
    return [...endpointSpecCounts.values()].map(({ spec }) => spec);
}

subscribe(EP_TOPIC, (entries) => {
    for (const [obj, w] of entries) {
        applyEndpointSpec(obj?.spec, w);
    }
    for (const fn of evCheckers.slice()) fn();
});

function addEvChecker(fn)    { evCheckers.push(fn); }
function removeEvChecker(fn) {
    const i = evCheckers.indexOf(fn);
    if (i >= 0) evCheckers.splice(i, 1);
}

// waitForGatheredSpec resolves when a gathered EndpointView exists whose
// spec.addresses (sorted) matches the given sorted addresses array.
function waitForGatheredSpec(addresses, port, protocol, timeoutMs = 10000) {
    const want = [...addresses].sort();
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            for (const spec of currentEndpointSpecs()) {
                if (spec?.port !== port || spec?.protocol !== protocol) continue;
                const got = [...(spec.addresses || [])].sort();
                if (got.length === want.length && got.every((v, i) => v === want[i])) {
                    done = true;
                    clearTimeout(timer);
                    removeEvChecker(tryResolve);
                    resolve();
                    return;
                }
            }
        }
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            removeEvChecker(tryResolve);
            reject(new Error(
                `timeout (${timeoutMs}ms) waiting for gathered EndpointView` +
                ` port=${port} proto=${protocol} addresses=${JSON.stringify(want)}`,
            ));
        }, timeoutMs);
        addEvChecker(tryResolve);
        tryResolve();
    });
}

// --- Domain helpers --------------------------------------------------------

function injectSvc(name) {
    publish("write-svc", [[{
        metadata: {
            name,
            namespace: TESTNS,
            annotations: { [CTRL_ANN]: "true" },
        },
        spec: {
            type: "ClusterIP",
            selector: { app: name },
            ports: [
                { name: "tcp-port", protocol: "TCP", port: 80,   targetPort: 8080  },
                { name: "udp-port", protocol: "UDP", port: 3478, targetPort: 33478 },
            ],
        },
    }, 1]]);
}

function injectES(name, svcName, endpoints) {
    publish("write-es", [[{
        metadata: {
            name,
            namespace: TESTNS,
            labels: { "kubernetes.io/service-name": svcName },
        },
        addressType: "IPv4",
        ports: [{ name: "http", port: 80, protocol: "TCP" }],
        endpoints,
    }, 1]]);
}

function makeEndpoint(address, ready = true) {
    return {
        addresses:  [address],
        conditions: { ready },
        hostname:   address.replace(/\./g, "-"),
        targetRef:  { kind: "Pod", name: `pod-${address.replace(/\./g, "-")}` },
    };
}

// --- Operator spec (mirrors endpointslice-controller-gather-spec.yaml) -----

const OPERATOR_SPEC = {
    apiVersion: "dcontroller.io/v1alpha1",
    kind: "Operator",
    metadata: { name: OP_NAME },
    spec: {
        controllers: [
            {
                name: "service-controller",
                sources: [{ apiGroup: "", kind: "Service" }],
                pipeline: [
                    { "@select": { "@exists": `$["metadata"]["annotations"]["${CTRL_ANN}"]` } },
                    {
                        "@project": {
                            metadata: {
                                name:      "$.metadata.name",
                                namespace: "$.metadata.namespace",
                            },
                            spec: {
                                serviceName: "$.metadata.name",
                                type:        "$.spec.type",
                                ports:       "$.spec.ports",
                            },
                        },
                    },
                    { "@unwind": "$.spec.ports" },
                    {
                        "@project": {
                            metadata: {
                                name: {
                                    "@concat": [
                                        "$.spec.serviceName",
                                        "-",
                                        "$.spec.ports.protocol",
                                        "-",
                                        "$.spec.ports.port",
                                    ],
                                },
                                namespace: "$.metadata.namespace",
                            },
                            spec: "$.spec",
                        },
                    },
                ],
                targets: [{ kind: "ServiceView" }],
            },
            {
                name: "endpointslice-controller",
                sources: [
                    { kind: "ServiceView" },
                    { apiGroup: "discovery.k8s.io", kind: "EndpointSlice" },
                ],
                pipeline: [
                    {
                        "@join": {
                            "@and": [
                                { "@eq": ["$.ServiceView.spec.serviceName", "$[\"EndpointSlice\"][\"metadata\"][\"labels\"][\"kubernetes.io/service-name\"]"] },
                                { "@eq": ["$.ServiceView.metadata.namespace", "$.EndpointSlice.metadata.namespace"] },
                            ],
                        },
                    },
                    {
                        "@project": {
                            metadata: {
                                name:      "$.ServiceView.metadata.name",
                                namespace: "$.ServiceView.metadata.namespace",
                            },
                            spec: {
                                serviceName: "$.ServiceView.spec.serviceName",
                                type:        "$.ServiceView.spec.type",
                                port:        "$.ServiceView.spec.ports.port",
                                targetPort:  "$.ServiceView.spec.ports.targetPort",
                                protocol:    "$.ServiceView.spec.ports.protocol",
                            },
                            endpoints: "$.EndpointSlice.endpoints",
                            id: {
                                name:       "$.ServiceView.spec.serviceName",
                                namespace:  "$.ServiceView.metadata.namespace",
                                type:       "$.ServiceView.spec.type",
                                protocol:   "$.ServiceView.spec.ports.protocol",
                                port:       "$.ServiceView.spec.ports.port",
                                targetPort: "$.ServiceView.spec.ports.targetPort",
                            },
                        },
                    },
                    { "@unwind": "$.endpoints" },
                    { "@select": { "@eq": ["$.endpoints.conditions.ready", true] } },
                    { "@unwind": "$.endpoints.addresses" },
                    { "@groupBy": ["$.id", "$.endpoints.addresses"] },
                    {
                        "@project": {
                            metadata: {
                                namespace: "$.key.namespace",
                                name: {
                                    "@concat": [
                                        "$.key.name",
                                        "-",
                                        "$.key.protocol",
                                        "-",
                                        "$.key.port",
                                    ],
                                },
                            },
                            spec: {
                                serviceName: "$.key.name",
                                type:        "$.key.type",
                                port:        "$.key.port",
                                targetPort:  "$.key.targetPort",
                                protocol:    "$.key.protocol",
                                addresses:   "$.values",
                            },
                        },
                    },
                ],
                targets: [{ kind: "EndpointView" }],
            },
        ],
    },
};

// --- Tests -----------------------------------------------------------------

describe("endpointslice controller — gathered output", (it) => {
    it("operator becomes ready", async () => {
        publish("write-operator", [[OPERATOR_SPEC, 1]]);
        await waitForOpReady(OP_NAME);
    });

    it("emits one gathered EndpointView per service port with all addresses", async () => {
        injectSvc("test-service-2");
        injectES("test-es-2", "test-service-2", [
            makeEndpoint("192.0.2.1"),
            makeEndpoint("192.0.2.2"),
        ]);

        // 2 ports (TCP 80, UDP 3478) → 2 EndpointViews, each with both addresses.
        const addrs = ["192.0.2.1", "192.0.2.2"];
        await waitForGatheredSpec(addrs, 80,   "TCP");
        await waitForGatheredSpec(addrs, 3478, "UDP");

        // Cleanup.
        publish("write-svc",      [[{ metadata: { name: "test-service-2", namespace: TESTNS } }, -1]]);
        publish("write-es",       [[{ metadata: { name: "test-es-2",      namespace: TESTNS } }, -1]]);
        publish("write-operator", [[{ metadata: { name: OP_NAME }              }, -1]]);
    });
}).then(() => exit());
