"use strict";

// Manager-level integration suite: operator lifecycle reconciliation and
// operator status reporting. Complements the per-example suites under
// examples/, which cover pipeline behavior of individual operators.

const { describe, sleep } = require("testing");
const { DControllerManager } = require("../lib/manager");
const { RuntimeConfig } = require("../lib/config");
const { createLogger } = require("log");

const OPERATOR_GVK = "dcontroller.io/v1alpha1/Operator";
const SVC_GVK      = "v1/Service";
const NS_GVK       = "v1/Namespace";
const TESTNS       = "manager-test";

// Start the manager (config comes from env vars set by the Go harness).
const config = new RuntimeConfig();
const runtimeConfig = config.makeFromEnv();
const manager = new DControllerManager({
    runtimeConfig,
    logger: createLogger("test-manager"),
});
manager.start();

// Writers for native K8s resources.
kubernetes.update("write-ns",       { gvk: NS_GVK });
kubernetes.update("write-operator", { gvk: OPERATOR_GVK });
kubernetes.update("write-svc",      { gvk: SVC_GVK });
kubernetes.patch("patch-svc",       { gvk: SVC_GVK });

// Watchers.
kubernetes.watch("watch-svc", { gvk: SVC_GVK, namespace: TESTNS });
kubernetes.watch("watch-op",  { gvk: OPERATOR_GVK });

// --- Operator-status checker -----------------------------------------------

const latestOpStatus = {}; // opName -> status object
const opCheckers = [];

subscribe("watch-op", (entries) => {
    for (const [obj, w] of entries) {
        const name = obj.metadata?.name;
        if (!name) continue;
        if (w > 0) {
            latestOpStatus[name] = obj.status || {};
        } else {
            delete latestOpStatus[name];
        }
    }
    for (const fn of opCheckers.slice()) fn();
});

function removeOpChecker(fn) {
    const idx = opCheckers.indexOf(fn);
    if (idx >= 0) opCheckers.splice(idx, 1);
}

// Resolves with the operator status once its Ready condition matches
// condStatus/reason and check(status) (when given) is satisfied.
function waitForOpStatus(opName, condStatus, reason, check, timeoutMs = 20000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            const status = latestOpStatus[opName];
            const cond = (status?.conditions || []).find(c => c.type === "Ready");
            if (cond?.status !== condStatus || cond?.reason !== reason) return;
            if (check && !check(status)) return;
            done = true;
            clearTimeout(timer);
            removeOpChecker(tryResolve);
            resolve(status);
        }
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            removeOpChecker(tryResolve);
            const cond = (latestOpStatus[opName]?.conditions || []).find(c => c.type === "Ready");
            reject(new Error(
                `timeout (${timeoutMs}ms) waiting for operator ${opName} ${condStatus}/${reason}` +
                ` (current=${cond?.status}/${cond?.reason})`,
            ));
        }, timeoutMs);
        opCheckers.push(tryResolve);
        tryResolve();
    });
}

// --- Service annotation tracking -------------------------------------------

const latestSvcAnns = {}; // svcName -> annotations map
const svcCheckers = [];

subscribe("watch-svc", (entries) => {
    for (const [obj, w] of entries) {
        const name = obj.metadata?.name;
        if (!name) continue;
        if (w > 0) {
            latestSvcAnns[name] = obj.metadata?.annotations || {};
        }
    }
    for (const fn of svcCheckers.slice()) fn();
});

function removeSvcChecker(fn) {
    const idx = svcCheckers.indexOf(fn);
    if (idx >= 0) svcCheckers.splice(idx, 1);
}

function waitForSvcSeen(svcName, timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            if (svcName in latestSvcAnns) {
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
            reject(new Error(`timeout (${timeoutMs}ms) waiting for service ${svcName} to appear`));
        }, timeoutMs);
        svcCheckers.push(tryResolve);
        tryResolve();
    });
}

function waitForAnnotation(svcName, key, expected, timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            if (latestSvcAnns[svcName]?.[key] === expected) {
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
                `timeout (${timeoutMs}ms) waiting for ${svcName} annotation ${key}="${expected}"` +
                ` (current="${latestSvcAnns[svcName]?.[key]}")`,
            ));
        }, timeoutMs);
        svcCheckers.push(tryResolve);
        tryResolve();
    });
}

// --- Domain helpers --------------------------------------------------------

// Annotates every Service in TESTNS with its spec.type under annotationKey.
function makeServiceTypeOperator(name, annotationKey) {
    return {
        apiVersion: "dcontroller.io/v1alpha1",
        kind: "Operator",
        metadata: { name },
        spec: {
            controllers: [{
                name: "svc-annotator",
                sources: [{ apiGroup: "", kind: "Service", namespace: TESTNS }],
                pipeline: [
                    { "@project": {
                        metadata: {
                            name: "$.metadata.name",
                            namespace: "$.metadata.namespace",
                            annotations: { [annotationKey]: "$.spec.type" },
                        },
                    } },
                ],
                targets: [{ apiGroup: "", kind: "Service", type: "Patcher" }],
            }],
        },
    };
}

// Fails at compile time: the pipeline uses an unknown operator.
function makeInvalidOperator(name) {
    return {
        apiVersion: "dcontroller.io/v1alpha1",
        kind: "Operator",
        metadata: { name },
        spec: {
            controllers: [{
                name: "broken",
                sources: [{ apiGroup: "", kind: "Service", namespace: TESTNS }],
                pipeline: [{ "@bogus": 1 }],
                targets: [{ apiGroup: "", kind: "Service", type: "Patcher" }],
            }],
        },
    };
}

// Every reconciliation fails at the target: the projected ConfigMap carries
// no namespace, so the update is rejected by the API server.
function makeRuntimeErrorOperator(name) {
    return {
        apiVersion: "dcontroller.io/v1alpha1",
        kind: "Operator",
        metadata: { name },
        spec: {
            controllers: [{
                name: "runtime-fail",
                sources: [{ apiGroup: "", kind: "Service", namespace: TESTNS }],
                pipeline: [
                    { "@project": {
                        apiVersion: "v1",
                        kind: "ConfigMap",
                        metadata: { name: "$.metadata.name" },
                        data: { x: "1" },
                    } },
                ],
                targets: [{ apiGroup: "", kind: "ConfigMap", type: "Updater" }],
            }],
        },
    };
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

function setSvcType(name, type) {
    publish("patch-svc", [[{
        metadata: { name, namespace: TESTNS },
        spec: { type },
    }, 1]]);
}

function deleteSvc(name) {
    publish("write-svc", [[{ metadata: { name, namespace: TESTNS } }, -1]]);
}

function deleteOperator(name) {
    publish("write-operator", [[{ metadata: { name } }, -1]]);
}

// --- Tests -----------------------------------------------------------------

describe("manager", (it) => {
    it("reconciles operator create, modify, and delete lifecycle", async () => {
        publish("write-ns", [[{ metadata: { name: TESTNS } }, 1]]);
        await sleep(500);
        injectSvc("lifecycle-svc");
        await waitForSvcSeen("lifecycle-svc");

        publish("write-operator", [[makeServiceTypeOperator("lifecycle-operator", "dcontroller.io/service-type"), 1]]);
        await waitForOpStatus("lifecycle-operator", "True", "Ready");
        await waitForAnnotation("lifecycle-svc", "dcontroller.io/service-type", "ClusterIP");

        // Modify: the operator spec switches to a v2 annotation key.
        publish("write-operator", [[makeServiceTypeOperator("lifecycle-operator", "dcontroller.io/service-type-v2"), 1]]);
        setSvcType("lifecycle-svc", "NodePort");
        await waitForAnnotation("lifecycle-svc", "dcontroller.io/service-type-v2", "NodePort");

        // Delete: further service changes must no longer be reconciled.
        deleteOperator("lifecycle-operator");
        await sleep(500);
        setSvcType("lifecycle-svc", "LoadBalancer");
        await sleep(1200);
        const ann = latestSvcAnns["lifecycle-svc"]?.["dcontroller.io/service-type-v2"];
        if (ann !== "NodePort") {
            throw new Error(`expected stale annotation "NodePort" after operator delete, got "${ann}"`);
        }
    });

    it("publishes NotReady status for invalid operator specs", async () => {
        publish("write-operator", [[makeInvalidOperator("invalid-operator"), 1]]);
        const status = await waitForOpStatus("invalid-operator", "False", "NotReady",
            s => (s.lastErrors || []).length > 0);
        if (!(status.lastErrors || []).length) {
            throw new Error("expected non-empty lastErrors on invalid operator");
        }
        deleteOperator("invalid-operator");
    });

    it("caps runtime errors to the last five messages", async () => {
        publish("write-operator", [[makeRuntimeErrorOperator("runtime-error-operator"), 1]]);
        for (let i = 0; i < 8; i++) {
            injectSvc(`runtime-error-svc-${i}`);
        }

        const status = await waitForOpStatus("runtime-error-operator", "False", "ReconciliationFailed",
            s => (s.lastErrors || []).length === 5);
        if ((status.lastErrors || []).length !== 5) {
            throw new Error(`expected lastErrors capped at 5, got ${(status.lastErrors || []).length}`);
        }

        // Cleanup.
        deleteOperator("runtime-error-operator");
        for (let i = 0; i < 8; i++) {
            deleteSvc(`runtime-error-svc-${i}`);
        }
        deleteSvc("lifecycle-svc");
    });
}).then(() => exit());
