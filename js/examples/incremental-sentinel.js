// incremental-sentinel.js — Incremental closed-loop reconciler.
//
// Implements the incremental reconciler circuit from the paper (Section 4.4):
//
//   δU = C^Δ(δY) − δY   incremental error signal
//   U  = δU + z^{-1}(U)  error accumulator (integrator in the controller)
//   δY = z^{-1}(U + W)   velocity-domain plant (pure delay)
//
// The DBSP runtime realises the z^{-1}(U) delay element automatically via the
// circuit's Delay nodes after incrementalisation.  In steady state (annotation
// already correct) U = 0 and no patches are emitted: O(|δY|) per tick.
// After a disturbance the circuit emits only the minimal correction.

const { createLogger } = require("log");
const logger = createLogger("examples.incremental-sentinel");

kubernetes.runtime.start();

// === Input: watch Service default/iperf-server ===
kubernetes.watch("services", {
    gvk:       "v1/Service",
    namespace: "default",
}, (entries) => {
    logger.info({ entries }, "input service watcher");
    return entries;
});

// === C^Δ: incrementalised desired-state controller ===
aggregate.compile(
    [
        // Filter to the target service only.
        { "@select": { "@eq": ["$.metadata.name", "iperf-server"] } },
        // Desired state: preserve name, namespace and spec; force the sentinel annotation.
        { "@project": {
            metadata: {
                name:      "$.metadata.name",
                namespace: "$.metadata.namespace",
                annotations: {
                    "dbsp-sentinel": "true",
                },
            },
            spec: "$.spec",
        }},
    ],
    { inputs: ["services"], outputs: ["desired-services"] }
).transform([
    // Explicit pair using topic names; the runtime maps them to
    // input_*/output_* node IDs.
    { name: "Reconciler", pairs: [["services", "desired-services"]] },
    { name: "Distincter" },
    { name: "Incrementalizer" },
]);

// === Output: apply U to the cluster via merge-patch ===
kubernetes.patch("desired-services", {
    gvk: "v1/Service",
}, (entries) => {
    logger.info({ entries }, "desired service watcher");
    return entries;
});
