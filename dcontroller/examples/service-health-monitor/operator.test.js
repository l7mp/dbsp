"use strict";

const { describe, sleep } = require("testing");
const { DControllerManager } = require("../../lib/manager");
const { RuntimeConfig } = require("../../lib/config");
const { createLogger } = require("log");

const OPERATOR_GVK = "dcontroller.io/v1alpha1/Operator";
// PodView is a view GVK — only available once the operator is ready.
const PODVIEW_GVK  = "svc-health-operator.view.dcontroller.io/v1alpha1/PodView";
const SVC_GVK      = "v1/Service";
const TESTNS       = "default";
const HEALTH_ANN   = "dcontroller.io/pod-ready";

// Start the manager (embedded API server address comes from env vars set by
// the Go harness via RunJSWithAPIServer).
const config = new RuntimeConfig();
const runtimeConfig = config.makeFromEnv();
const manager = new DControllerManager({
    runtimeConfig,
    logger: createLogger("test-manager"),
});
manager.start();

// Updaters for native K8s resources (registered at load time).
const writeOperator = kubernetes.update("write-operator", { gvk: OPERATOR_GVK });
const writeSvc      = kubernetes.update("write-svc",      { gvk: SVC_GVK });

// Watchers.
kubernetes.watch("watch-svc", { gvk: SVC_GVK, namespace: TESTNS });
kubernetes.watch("watch-op",  { gvk: OPERATOR_GVK });

// --- Operator-status checker -----------------------------------------------

const opCheckers = [];
subscribe("watch-op", (entries) => {
    for (const fn of opCheckers.slice()) fn(entries);
});

function waitForOpStatus(opName, condStatus, reason, timeoutMs = 10000) {
    return new Promise((resolve, reject) => {
        let done = false;
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            const idx = opCheckers.indexOf(checker);
            if (idx >= 0) opCheckers.splice(idx, 1);
            reject(new Error(`timeout (${timeoutMs}ms) waiting for operator ${opName} ${condStatus}/${reason}`));
        }, timeoutMs);
        function checker(entries) {
            if (done) return;
            for (const [obj, w] of entries) {
                if (w <= 0 || obj.metadata.name !== opName) continue;
                const cond = (obj.status?.conditions || []).find(c => c.type === "Ready");
                if (cond?.status === condStatus && cond?.reason === reason) {
                    done = true;
                    clearTimeout(timer);
                    const idx = opCheckers.indexOf(checker);
                    if (idx >= 0) opCheckers.splice(idx, 1);
                    resolve(obj);
                    return;
                }
            }
        }
        opCheckers.push(checker);
    });
}

// --- Service annotation state tracking ------------------------------------

const latestSvcState = {}; // svcName -> { rv, ann }
const svcCheckers  = [];

subscribe("watch-svc", (entries) => {
    for (const [obj, w] of entries) {
        const name = obj.metadata?.name;
        const rv = obj.metadata?.resourceVersion;
        if (!name || !rv) continue;
        if (w > 0) {
            latestSvcState[name] = {
                rv,
                ann: obj.metadata?.annotations?.[HEALTH_ANN],
            };
        } else if (latestSvcState[name]?.rv === rv) {
            delete latestSvcState[name];
        }
    }
    for (const fn of svcCheckers.slice()) fn();
});

function addSvcChecker(fn)    { svcCheckers.push(fn); }
function removeSvcChecker(fn) {
    const idx = svcCheckers.indexOf(fn);
    if (idx >= 0) svcCheckers.splice(idx, 1);
}

function waitForAnnotation(svcName, expected, timeoutMs = 8000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            if (latestSvcState[svcName]?.ann === expected) {
                done = true;
                clearTimeout(timer);
                removeSvcChecker(tryResolve);
                resolve();
            }
        }
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            removeSvcChecker(tryResolve);
            reject(new Error(
                `timeout (${timeoutMs}ms) waiting for ${svcName} annotation "${expected}"` +
                ` (current="${latestSvcState[svcName]?.ann}")`,
            ));
        }, timeoutMs);
        addSvcChecker(tryResolve);
        tryResolve();
    });
}

function waitForNoAnnotation(svcName, timeoutMs = 8000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            if (svcName in latestSvcState && latestSvcState[svcName]?.ann === undefined) {
                done = true;
                clearTimeout(timer);
                removeSvcChecker(tryResolve);
                resolve();
            }
        }
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            removeSvcChecker(tryResolve);
            reject(new Error(
                `timeout (${timeoutMs}ms) waiting for ${svcName} to have no annotation` +
                ` (current="${latestSvcState[svcName]?.ann}")`,
            ));
        }, timeoutMs);
        addSvcChecker(tryResolve);
        tryResolve();
    });
}

// --- Domain helpers --------------------------------------------------------

function injectOperator(spec) {
    publish("write-operator", [[spec, 1]]);
}

function injectSvc(name) {
    publish("write-svc", [[{
        metadata: { name, namespace: TESTNS },
        spec: {
            selector: { app: name },
            ports: [{ port: 80, targetPort: 8080, protocol: "TCP" }],
        },
    }, 1]]);
}

function deleteSvc(name) {
    publish("write-svc", [[{ metadata: { name, namespace: TESTNS } }, -1]]);
}

function makePodView(podName, appLabel, ready) {
    const readyStatus = ready ? "True" : "False";
    return {
        apiVersion: "svc-health-operator.view.dcontroller.io/v1alpha1",
        kind: "PodView",
        metadata: {
            name: podName,
            namespace: TESTNS,
            labels: { app: appLabel },
        },
        status: {
            conditions: [{ type: "Ready", status: readyStatus }],
        },
    };
}

const podViews = {};

function upsertPodView(podName, appLabel, ready) {
    const podView = makePodView(podName, appLabel, ready);
    podViews[podName] = podView;
    publish("write-podview", [[podView, 1]]);
}

function deletePodView(podName) {
    const podView = podViews[podName] || {
        apiVersion: "svc-health-operator.view.dcontroller.io/v1alpha1",
        kind: "PodView",
        metadata: { name: podName, namespace: TESTNS },
    };
    publish("write-podview", [[podView, -1]]);
    delete podViews[podName];
}

// --- Operator spec (mirrors svc-health-operator-test.yaml) -----------------

const OPERATOR_SPEC = {
    apiVersion: "dcontroller.io/v1alpha1",
    kind: "Operator",
    metadata: { name: "svc-health-operator" },
    spec: {
        controllers: [
            {
                name: "pod-health-monitor",
                // No apiGroup → view GVK (svc-health-operator.view.dcontroller.io)
                sources: [{ kind: "PodView" }],
                pipeline: [
                    {
                        "@project": {
                            metadata: {
                                name: "$.metadata.labels.app",
                                namespace: "$.metadata.namespace",
                            },
                            pods: {
                                podName: "$.metadata.name",
                                ready: "$.status.conditions[?(@.type=='Ready')].status",
                            },
                        },
                    },
                    { "@groupBy": ["$.metadata", "$.pods"] },
                    { "@project": { metadata: "$.key", pods: "$.values" } },
                ],
                targets: [{ kind: "HealthView", type: "Updater" }],
            },
            {
                name: "svc-health-monitor",
                sources: [
                    { kind: "HealthView" },
                    { apiGroup: "", kind: "Service" },
                ],
                pipeline: [
                    {
                        "@join": {
                            "@and": [
                                { "@eq": ["$.HealthView.metadata.name",      "$.Service.metadata.name"] },
                                { "@eq": ["$.HealthView.metadata.namespace", "$.Service.metadata.namespace"] },
                            ],
                        },
                    },
                    {
                        "@project": {
                            metadata: {
                                name:      "$.Service.metadata.name",
                                namespace: "$.Service.metadata.namespace",
                                annotations: {
                                    [HEALTH_ANN]: {
                                        "@concat": [
                                            { "@string": { "@len": { "@filter": [{ "@eq": ["$$.ready", "True"] }, "$.HealthView.pods"] } } },
                                            "/",
                                            { "@string": { "@len": "$.HealthView.pods" } },
                                        ],
                                    },
                                },
                            },
                        },
                    },
                ],
                targets: [{ apiGroup: "", kind: "Service", type: "Patcher" }],
            },
        ],
    },
};

// --- Tests -----------------------------------------------------------------

describe("service-health-monitor", (it) => {
    // writePodView is created lazily in the first test, after view GVKs are
    // registered by the operator with the embedded API server.
    let writePodView;

    it("operator becomes ready", async () => {
        injectOperator(OPERATOR_SPEC);
        await waitForOpStatus("svc-health-operator", "True", "Ready");
        // Now the embedded API server knows about PodView — safe to register the updater.
        writePodView = kubernetes.update("write-podview", { gvk: PODVIEW_GVK });
        await sleep(100);
    });

    it("adds health annotation when pods are ready", async () => {
        injectSvc("web-app");

        upsertPodView("web-app-pod-1", "web-app", true);
        upsertPodView("web-app-pod-2", "web-app", true);

        await waitForAnnotation("web-app", "2/2");
    });

    it("updates annotation when a pod becomes unhealthy", async () => {
        upsertPodView("web-app-pod-3", "web-app", false);
        await waitForAnnotation("web-app", "2/3");
    });

    it("updates annotation when the unhealthy pod recovers", async () => {
        upsertPodView("web-app-pod-3", "web-app", true);
        await waitForAnnotation("web-app", "3/3");
    });

    it("removes annotation when all pods are deleted", async () => {
        deletePodView("web-app-pod-1");
        deletePodView("web-app-pod-2");
        deletePodView("web-app-pod-3");

        await waitForNoAnnotation("web-app");
    });

    it("does not annotate a service without matching pods", async () => {
        injectSvc("other-app");
        // Give the operator time to process the new service; annotation must stay absent.
        await sleep(1000);
        const ann = latestSvcState["other-app"]?.ann;
        if (ann !== undefined) {
            throw new Error(`expected no annotation on other-app, got "${ann}"`);
        }

        // Cleanup.
        deleteSvc("web-app");
        deleteSvc("other-app");
        publish("write-operator", [[{ metadata: { name: "svc-health-operator" } }, -1]]);
    });
}).then(() => exit());
