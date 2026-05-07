// naive-sentinel.js — Naive (state-of-the-world) control loop.
//
// Implements the naive system from the paper (Section 3):
//
//   U = C(Y)           controller: compute desired state
//   Y = z^{-1}(U + W)  plant: replacement semantics, unit delay
//
// On every watch event the pipeline computes C(Y) — the full desired
// annotation state — and the patcher applies it directly.  This is
// O(|Y|) per tick: every service change triggers an unconditional patch,
// regardless of whether the annotation already has the correct value.
//
// NOTE: because @project maps both the old and the new version of the
// service to the same desired object {name, ns, annotations:{dbsp-sentinel:"true"}}
// when only the annotation changes, the two Z-set entries cancel (−1 + 1 = 0)
// and no patch is emitted for an annotation-removal disturbance.  

// === Input: list Service snapshots on each watch tick ===
kubernetes.list("services", {
    gvk:       "v1/Service",
    namespace: "default",
}, (entries) => {
    console.log("=== input service watcher === ", entries);
    return entries;
});

// === C(Y): desired-state controller (non-incremental, SotW) ===
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
);   // No .transform("Incrementalizer") — SotW: full desired state each tick.

// === Output: apply U to the cluster via merge-patch ===
kubernetes.patch("desired-services", {
    gvk: "v1/Service",
}, (entries) => {
    console.log("=== desired service watcher === ", entries);
    return entries;
});
