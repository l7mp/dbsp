// pod-log-filter.js — watch pods by label selector and print error-level log entries.
//
// Usage:
//   dbsp examples/pod-log-filter.js <label-key>:<label-value>[,<key>:<value>...]
//
// Example:
//   dbsp examples/pod-log-filter.js app:my-app
//   dbsp examples/pod-log-filter.js app:my-app,env:prod

const args = (process && process.argv ? process.argv : []).slice(2);

function parseLabels(s) {
    const result = {};
    for (const pair of (s || "").split(",")) {
        const idx = pair.indexOf(":");
        if (idx < 0) continue;
        const k = pair.slice(0, idx).trim();
        const v = pair.slice(idx + 1).trim();
        if (k) result[k] = v;
    }
    return result;
}

const labels = parseLabels(args[0]);
if (Object.keys(labels).length === 0) {
    throw new Error("Usage: dbsp pod-log-filter.js <label-key>:<label-value>[,...]");
}

// Watch Pods matching the label selector and publish raw pod documents.
kubernetes.watch("pods-raw", { gvk: "v1/Pod", labels });

// Project to pod identity fields and deduplicate incrementally.
// @distinct ensures the subscribe callback fires exactly once per pod
// appearance (+1) and once per disappearance (-1), even when the watch
// stream replays existing pods or delivers update events for the same pod.
aggregate.compile([
    { "@project": { "name": "$.metadata.name", "namespace": "$.metadata.namespace" } },
    { "@distinct": {} },
], {
    inputs: "pods-raw",
    output: "pods",
}).transform("Incrementalizer").validate();

// Compile a shared filter circuit: parse each raw log line as JSONL and
// keep only documents whose "level" field equals "error".
aggregate.compile([
    { "@select": { "@eq": ["$.level", "error"] } },
], {
    inputs: "pod-logs-raw",
    output: "error-logs",
}).transform("Incrementalizer").validate();

// For each new pod, start a log stream.  The callback parses raw log lines
// as JSONL and publishes the result; the shared filter circuit above then
// drops non-error documents before they reach the subscribe sink.
subscribe("pods", (entries) => {
    for (const [pod, weight] of entries) {
        if (weight > 0) {
            kubernetes.log("pod-logs-raw", {
                name:      pod.name,
                namespace: pod.namespace || "default",
            }, (entries) => format.jsonl(entries));
        }
        // weight < 0: pod gone; the LogProducer will drain on EOF and back
        // off until the overall context is cancelled.
    }
});

// Print every error-level log document as it arrives.
subscribe("error-logs", (entries) => {
    for (const [doc, weight] of entries) {
        if (weight > 0) {
            console.log("[ERROR]", JSON.stringify(doc));
        }
    }
});
