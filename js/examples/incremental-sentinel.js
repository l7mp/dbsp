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
//
// For the linear @project stage C^Δ = C (LTI operators are self-incremental,
// DBSP Theorem 3.1), so .transform("Incrementalizer") wraps the circuit in D ∘ C ∘ I,
// which is the canonical incremental form.
//
// NOTE: the same Z-set cancellation caveat as naive-sentinel.js applies when
// only the annotation field changes (C(old) = C(new) for the projected
// annotation subspace).  The error term δU collapses to zero for pure
// annotation-removal events.  To observe full disturbance rejection, either
// include a version-bearing field in the projection or extend the pipeline
// with a @select stage that distinguishes "annotation present" from "absent".

// === Input: watch Service default/iperf-server ===
producer.kubernetes.watch({
    gvk:       "v1/Service",
    namespace: "default",
    topic:     "services",
}, (entries) => {
    console.log("=== input service watcher === ", entries);
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
    { inputs: ["services"], output: "desired-services" }
).transform("Incrementalizer").validate();   // Incremental form: D ∘ C ∘ I.

// === Output: apply U to the cluster via merge-patch ===
consumer.kubernetes.patcher({
    gvk:   "v1/Service",
    topic: "desired-services",
}, (entries) => {
    console.log("=== desired service watcher === ", entries);
    return entries;
});
