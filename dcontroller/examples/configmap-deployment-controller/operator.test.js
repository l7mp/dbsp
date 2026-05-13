"use strict";

const { describe } = require("testing");
const { DControllerManager } = require("../../lib/manager");
const { RuntimeConfig } = require("../../lib/config");
const { createLogger } = require("log");

const OPERATOR_GVK = "dcontroller.io/v1alpha1/Operator";
const CM_GVK       = "v1/ConfigMap";
const DEPLOY_GVK   = "apps/v1/Deployment";
const CD_GVK       = "dcontroller.io/v1alpha1/ConfigDeployment";
const TESTNS       = "default";
const ANN          = "dcontroller.io/configmap-version";

// Start the manager from env-var config (Go harness sets DCONTROLLER_KUBECONFIG
// and DCONTROLLER_API_SERVER_ENABLED=false before launching this script).
const config = new RuntimeConfig();
const runtimeConfig = config.makeFromEnv();
const manager = new DControllerManager({
    runtimeConfig,
    logger: createLogger("test-manager"),
});
manager.start();

// Updater consumers — write test fixtures into K8s.
const writeOperator = kubernetes.update("write-operator", { gvk: OPERATOR_GVK });
const writeCM       = kubernetes.update("write-cm",       { gvk: CM_GVK });
const writeDeploy   = kubernetes.update("write-deploy",   { gvk: DEPLOY_GVK });
const writeCD       = kubernetes.update("write-cd",       { gvk: CD_GVK });

// Watcher producers — deliver K8s change events onto named topics.
kubernetes.watch("watch-deploy", { gvk: DEPLOY_GVK, namespace: TESTNS });
kubernetes.watch("watch-cm",     { gvk: CM_GVK,     namespace: TESTNS });
kubernetes.watch("watch-op",     { gvk: OPERATOR_GVK });

// --- Condition checker infrastructure for the Operator topic ---------------
//
// A single persistent subscribe() feeds a dynamic set of checker functions.

const opCheckers = [];

subscribe("watch-op", (entries) => {
    for (const fn of opCheckers.slice()) fn(entries);
});

function addOpChecker(fn)    { opCheckers.push(fn); }
function removeOpChecker(fn) {
    const idx = opCheckers.indexOf(fn);
    if (idx >= 0) opCheckers.splice(idx, 1);
}

function waitForOp(check, timeoutMs) {
    return new Promise((resolve, reject) => {
        let done = false;
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            removeOpChecker(checker);
            reject(new Error(`timeout (${timeoutMs}ms) waiting on watch-op`));
        }, timeoutMs);
        function checker(entries) {
            if (done) return;
            for (const [obj, w] of entries) {
                if (w <= 0) continue;
                const result = check(obj);
                if (result !== undefined) {
                    done = true;
                    clearTimeout(timer);
                    removeOpChecker(checker);
                    resolve(result);
                    return;
                }
            }
        }
        addOpChecker(checker);
    });
}

// --- State tracking for ConfigMaps and Deployments -------------------------
//
// latestCmState[name]  — latest seen ConfigMap state: { rv }
// latestDepState[name] — latest seen Deployment state: { rv, ann }
//
// stateCheckers are notified on every CM or Deployment watch event so that
// waitForDepMatchesCM / waitForDepNoAnnotation can react to either side changing.

const latestCmState  = {};
const latestDepState = {};
const stateCheckers = [];

function applyLatestByRV(state, obj, w, project) {
    const name = obj.metadata?.name;
    const rv = obj.metadata?.resourceVersion;
    if (!name || !rv) return;

    if (w > 0) {
        state[name] = project(obj, rv);
        return;
    }
    if (state[name]?.rv === rv) delete state[name];
}

subscribe("watch-cm", (entries) => {
    for (const [obj, w] of entries) {
        applyLatestByRV(latestCmState, obj, w, (_, rv) => ({ rv }));
    }
    for (const fn of stateCheckers.slice()) fn();
});

subscribe("watch-deploy", (entries) => {
    for (const [obj, w] of entries) {
        applyLatestByRV(latestDepState, obj, w, (cur, rv) => ({
            rv,
            ann: cur.spec?.template?.metadata?.annotations?.[ANN],
        }));
    }
    for (const fn of stateCheckers.slice()) fn();
});

function addStateChecker(fn)    { stateCheckers.push(fn); }
function removeStateChecker(fn) {
    const idx = stateCheckers.indexOf(fn);
    if (idx >= 0) stateCheckers.splice(idx, 1);
}

// waitForDepMatchesCM resolves when deployment's ANN annotation equals the CM's
// current resourceVersion.  Both values must be present and equal.
function waitForDepMatchesCM(depName, cmName, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            const cmRV = latestCmState[cmName]?.rv;
            const depAnn = latestDepState[depName]?.ann;
            if (cmRV !== undefined && typeof depAnn === "string" && depAnn === cmRV) {
                done = true;
                clearTimeout(timer);
                removeStateChecker(tryResolve);
                resolve();
            }
        }
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            removeStateChecker(tryResolve);
            reject(new Error(
                `timeout (${timeoutMs}ms) waiting for ${depName} annotation to match CM ${cmName}` +
                ` (cmRV=${latestCmState[cmName]?.rv}, depAnn=${latestDepState[depName]?.ann})`,
            ));
        }, timeoutMs);
        addStateChecker(tryResolve);
        tryResolve();
    });
}

// waitForDepNoAnnotation resolves when the deployment has been received at least
// once AND the ANN annotation is absent from spec.template.metadata.annotations.
function waitForDepNoAnnotation(depName, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
        let done = false;
        function tryResolve() {
            if (done) return;
            // Key present (seen at least once) and annotation value is undefined (absent).
            if (depName in latestDepState && latestDepState[depName]?.ann === undefined) {
                done = true;
                clearTimeout(timer);
                removeStateChecker(tryResolve);
                resolve();
            }
        }
        const timer = setTimeout(() => {
            if (done) return;
            done = true;
            removeStateChecker(tryResolve);
            reject(new Error(
                `timeout (${timeoutMs}ms) waiting for ${depName} to have no annotation` +
                ` (current=${latestDepState[depName]?.ann})`,
            ));
        }, timeoutMs);
        addStateChecker(tryResolve);
        tryResolve();
    });
}

// --- Domain helpers --------------------------------------------------------

function injectOperator(spec) {
    publish("write-operator", [[spec, 1]]);
}

function deleteOperator(name) {
    publish("write-operator", [[{ metadata: { name } }, -1]]);
}

function injectCM(name) {
    publish("write-cm", [[{
        metadata: { name, namespace: TESTNS },
        data: { key1: "value1", key2: "value2" },
    }, 1]]);
}

function updateCM(name, data) {
    publish("write-cm", [[{
        metadata: { name, namespace: TESTNS },
        data,
    }, 1]]);
}

function deleteCM(name) {
    publish("write-cm", [[{ metadata: { name, namespace: TESTNS } }, -1]]);
}

function injectDeploy(name) {
    publish("write-deploy", [[{
        metadata: { name, namespace: TESTNS },
        spec: {
            replicas: 1,
            selector: { matchLabels: { app: name } },
            template: {
                metadata: { labels: { app: name } },
                spec: { containers: [{ name: "app", image: "nginx:latest" }] },
            },
        },
    }, 1]]);
}

function deleteDeploy(name) {
    publish("write-deploy", [[{ metadata: { name, namespace: TESTNS } }, -1]]);
}

function injectCD(name, configMap, deployment) {
    publish("write-cd", [[{
        apiVersion: "dcontroller.io/v1alpha1",
        kind: "ConfigDeployment",
        metadata: { name, namespace: TESTNS },
        spec: { configMap, deployment },
    }, 1]]);
}

function deleteCD(name) {
    publish("write-cd", [[{ metadata: { name, namespace: TESTNS } }, -1]]);
}

function waitForOperatorReady(opName, timeoutMs = 5000) {
    return waitForOp((obj) => {
        if (obj.metadata.name !== opName) return undefined;
        const cond = (obj.status?.conditions || []).find(c => c.type === "Ready");
        if (cond?.status === "True" && cond?.reason === "Ready") return obj;
    }, timeoutMs);
}

// --- Operator spec ---------------------------------------------------------

const OPERATOR_SPEC = {
    apiVersion: "dcontroller.io/v1alpha1",
    kind: "Operator",
    metadata: { name: "configdep-operator" },
    spec: {
        controllers: [{
            name: "configmap-controller",
            sources: [
                { apiGroup: "",               kind: "ConfigMap"       },
                { apiGroup: "apps",           kind: "Deployment"      },
                { apiGroup: "dcontroller.io", kind: "ConfigDeployment" },
            ],
            pipeline: [
                {
                    "@join": {
                        "@and": [
                            { "@eq": ["$.ConfigMap.metadata.name",      "$.ConfigDeployment.spec.configMap"] },
                            { "@eq": ["$.Deployment.metadata.name",     "$.ConfigDeployment.spec.deployment"] },
                            { "@eq": ["$.ConfigMap.metadata.namespace", "$.Deployment.metadata.namespace"] },
                            { "@eq": ["$.ConfigMap.metadata.namespace", "$.ConfigDeployment.metadata.namespace"] },
                        ],
                    },
                },
                {
                    "@project": {
                        metadata: {
                            name:      "$.Deployment.metadata.name",
                            namespace: "$.Deployment.metadata.namespace",
                        },
                        spec: {
                            template: {
                                metadata: {
                                    annotations: { [ANN]: "$.ConfigMap.metadata.resourceVersion" },
                                },
                            },
                        },
                    },
                },
            ],
            targets: [{ apiGroup: "apps", kind: "Deployment", type: "Patcher" }],
        }],
    },
};

// --- Tests -----------------------------------------------------------------

describe("configmap-deployment controller", (it) => {
    it("operator becomes ready", async () => {
        injectOperator(OPERATOR_SPEC);
        await waitForOperatorReady("configdep-operator", 10000);
    });

    it("annotates first deployment when first ConfigDeployment is created", async () => {
        injectDeploy("test-deployment-1");
        injectDeploy("test-deployment-2");
        injectCM("test-configmap-1");
        injectCM("test-configmap-2");
        injectCD("test-dep-config-1", "test-configmap-1", "test-deployment-1");

        await waitForDepMatchesCM("test-deployment-1", "test-configmap-1");
        await waitForDepNoAnnotation("test-deployment-2");
    });

    it("annotates second deployment when second ConfigDeployment is created", async () => {
        injectCD("test-dep-config-2", "test-configmap-2", "test-deployment-2");
        await waitForDepMatchesCM("test-deployment-2", "test-configmap-2");
    });

    it("handles a third deployment referring to the same ConfigMap", async () => {
        injectDeploy("test-deployment-3");
        injectCD("test-dep-config-3", "test-configmap-1", "test-deployment-3");

        await waitForDepMatchesCM("test-deployment-1", "test-configmap-1");
        await waitForDepMatchesCM("test-deployment-2", "test-configmap-2");
        await waitForDepMatchesCM("test-deployment-3", "test-configmap-1");
    });

    it("updates annotations when the ConfigMap is updated", async () => {
        updateCM("test-configmap-1", { key1: "value1", key2: "value2", key3: "value3" });

        await waitForDepMatchesCM("test-deployment-1", "test-configmap-1");
        await waitForDepMatchesCM("test-deployment-2", "test-configmap-2");
        await waitForDepMatchesCM("test-deployment-3", "test-configmap-1");
    });

    it("removes annotations when the ConfigMap is deleted", async () => {
        deleteCM("test-configmap-1");

        await waitForDepNoAnnotation("test-deployment-1");
        await waitForDepMatchesCM("test-deployment-2", "test-configmap-2");
        await waitForDepNoAnnotation("test-deployment-3");

        // Cleanup.
        deleteOperator("configdep-operator");
        deleteDeploy("test-deployment-1");
        deleteDeploy("test-deployment-2");
        deleteDeploy("test-deployment-3");
        deleteCM("test-configmap-2");
        deleteCD("test-dep-config-1");
        deleteCD("test-dep-config-2");
        deleteCD("test-dep-config-3");
    });
}).then(() => exit());
