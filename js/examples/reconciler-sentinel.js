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
// DBSP Theorem 3.1), so the optimizer applies transforms in canonical order:
// Rewriter -> Reconciler -> Regularizer -> Incrementalizer.
//
// NOTE: the same Z-set cancellation caveat as naive-sentinel.js applies when
// only the annotation field changes (C(old) = C(new) for the projected
// annotation subspace).  The error term δU collapses to zero for pure
// annotation-removal events.  To observe full disturbance rejection, either
// include a version-bearing field in the projection or extend the pipeline
// with a @select stage that distinguishes "annotation present" from "absent".

kubernetes.runtime.start();

// === Input: watch Service default/iperf-server ===
kubernetes.watch("services", {
    gvk:       "v1/Service",
    namespace: "default",
}, (entries) => {
    console.log("=== input service watcher === ", entries);
    return entries;
});

// === C^Δ: incrementalised desired-state controller ===
aggregate.compile(
    [
        // Filter to the target service only.
        { "@select": { "@eq": ["$.metadata.name", "iperf-server"] } },
        // Desired state: keep the full Service object and only enforce annotation.
        //
        // Why full object + updater matters here:
        // Reconciler emits negative deltas too; patcher interprets delete-side deltas as
        // field removals, which can invalidate immutable/required Service fields.
        // Updater applies full objects, so reconciler output remains valid.
        { "@project": [
            { "$.": "$." },
            { "$.metadata.annotations.dbsp-sentinel": "true" },
        ]},
    ],
    { inputs: ["services"], outputs: ["desired-services"] }
).transform("Optimizer", {
    // Explicit pair using topic names; runtime maps them to input_*/output_* node IDs.
    pairs: [["services", "desired-services"]],
}).validate();

// === Output: apply U to the cluster via full-object update ===
kubernetes.update("desired-services", {
    gvk: "v1/Service",
}, (entries) => {
    console.log("=== desired service watcher === ", entries);
    return entries;
});
