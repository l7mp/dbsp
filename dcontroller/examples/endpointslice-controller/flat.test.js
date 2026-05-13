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
const OP_NAME      = "ep-flat-op";
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
//
// We subscribe directly to the circuit output topic so that assertions work
// even before EndpointView objects propagate through the embedded API server.

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

// waitForEndpointSpec resolves when an EndpointView with the matching flat
// fields (single address per entry) is present.
function waitForEndpointSpec(address, port, protocol, timeoutMs = 10000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            for (const spec of currentEndpointSpecs()) {
                if (spec?.address === address && spec?.port === port && spec?.protocol === protocol) {
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
                `timeout (${timeoutMs}ms) waiting for EndpointView` +
                ` addr=${address} port=${port} proto=${protocol}`,
            ));
        }, timeoutMs);
        addEvChecker(tryResolve);
        tryResolve();
    });
}

// waitForNoEndpointAddress resolves when no EndpointView with the given
// address remains in the set.
function waitForNoEndpointAddress(address, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            const present = currentEndpointSpecs().some(s => s?.address === address);
            if (!present) {
                done = true;
                clearTimeout(timer);
                removeEvChecker(tryResolve);
                resolve();
            }
        }
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            removeEvChecker(tryResolve);
            reject(new Error(`timeout (${timeoutMs}ms) waiting for address ${address} to disappear`));
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
                { name: "tcp-port", protocol: "TCP",  port: 80,   targetPort: 8080  },
                { name: "udp-port", protocol: "UDP",  port: 3478, targetPort: 33478 },
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

function deleteES(name) {
    publish("write-es", [[{ metadata: { name, namespace: TESTNS } }, -1]]);
}

function makeEndpoint(address, ready = true) {
    return {
        addresses:  [address],
        conditions: { ready },
        hostname:   address.replace(/\./g, "-"),
        targetRef:  { kind: "Pod", name: `pod-${address.replace(/\./g, "-")}` },
    };
}

// --- Operator spec (mirrors endpointslice-controller-spec.yaml) ------------

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
                        },
                    },
                    { "@unwind": "$.endpoints" },
                    { "@select": { "@eq": ["$.endpoints.conditions.ready", true] } },
                    { "@unwind": "$.endpoints.addresses" },
                    {
                        "@project": {
                            metadata: { namespace: "$.metadata.namespace" },
                            spec: {
                                serviceName: "$.spec.serviceName",
                                type:        "$.spec.type",
                                port:        "$.spec.port",
                                targetPort:  "$.spec.targetPort",
                                protocol:    "$.spec.protocol",
                                address:     "$.endpoints.addresses",
                            },
                        },
                    },
                    {
                        "@project": [
                            { "$.": "$." },
                            { metadata: { name: { "@concat": ["$.spec.serviceName", "-", { "@hash": "$.spec" }] }, namespace: "$.metadata.namespace" } },
                        ],
                    },
                ],
                targets: [{ kind: "EndpointView" }],
            },
        ],
    },
};

// --- Tests -----------------------------------------------------------------

describe("endpointslice controller — flat output", (it) => {
    it("operator becomes ready", async () => {
        publish("write-operator", [[OPERATOR_SPEC, 1]]);
        await waitForOpReady(OP_NAME);
    });

    it("emits one flat EndpointView per endpoint address and service port", async () => {
        injectSvc("test-service-1");
        injectES("test-es-1", "test-service-1", [
            makeEndpoint("192.0.2.1"),
            makeEndpoint("192.0.2.2"),
        ]);

        // 2 addresses × 2 ports (TCP 80, UDP 3478) = 4 EndpointViews.
        await waitForEndpointSpec("192.0.2.1", 80,   "TCP");
        await waitForEndpointSpec("192.0.2.1", 3478, "UDP");
        await waitForEndpointSpec("192.0.2.2", 80,   "TCP");
        await waitForEndpointSpec("192.0.2.2", 3478, "UDP");
    });

    it("updates EndpointViews when an endpoint address changes", async () => {
        // Replace address 192.0.2.1 with 192.0.2.3 in the EndpointSlice.
        deleteES("test-es-1");
        injectES("test-es-1", "test-service-1", [
            makeEndpoint("192.0.2.3"),
            makeEndpoint("192.0.2.2"),
        ]);

        await waitForEndpointSpec("192.0.2.3", 80,   "TCP");
        await waitForEndpointSpec("192.0.2.3", 3478, "UDP");

        // Cleanup.
        publish("write-svc",      [[{ metadata: { name: "test-service-1", namespace: TESTNS } }, -1]]);
        deleteES("test-es-1");
        publish("write-operator", [[{ metadata: { name: OP_NAME }              }, -1]]);
    });
}).then(() => exit());
