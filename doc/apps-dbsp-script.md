# DBSP Script

The `dbsp` CLI in `js/` is the lightweight scripting environment of this repository. It is the
fastest way to try DBSP from the command line, compile SQL queries and aggregation pipelines,
inspect incremental behaviour, and connect a circuit to Kubernetes resources without writing Go.

Under the hood, `dbsp` runs a JavaScript file in an embedded Goja VM and attaches that VM to one
background DBSP runtime. The script defines circuits and topic wiring, the runtime moves deltas
between producers and consumers, and the process exits automatically when the queues drain or when
`cancel()` stops the current execution context.

The example scripts live in `js/examples/`.

## Build and run

If you just want the CLI, install it directly:

```bash
go install github.com/l7mp/dbsp/js@latest
```

From a local checkout, build it with:

```bash
cd js
make build
```

This writes the binary to `js/bin/dbsp`.

Run a script from the repository root:

```bash
./js/bin/dbsp js/examples/join_project.js
./js/bin/dbsp -v js/examples/observer-demo.js
```

The current implementation runs one script file at a time. There is no interactive REPL yet.

The CLI supports `--loglevel debug|info|warn|error` and the shorthand `-v`, which enables debug
logging.

## Node-style runtime compatibility

The DBSP JS runtime enables a CommonJS module system (`require`) with a curated Node-compatible
module set from `goja_nodejs`.

Available modules and globals include:

- `console` (`log`, `info`, `debug`, `warn`, `error`)
- `Buffer`
- `URL` and `URLSearchParams`
- `process.env`
- `util` (`util.format`)
- `assert` (built-in helper module)
- `fs` and `fs.promises` (read/write filesystem access)
- `timers/promises` (`setTimeout(ms, value)`)
- `@dbsp/test` (lightweight testing helpers, including `sleep`)

`node:` aliases are also available for these modules, for example:

- `require("node:assert")`
- `require("node:fs")`
- `require("node:fs/promises")`
- `require("node:timers/promises")`

Example:

```js
const assert = require("node:assert");
const fs = require("node:fs");
const { setTimeout } = require("node:timers/promises");

fs.writeFileSync("result.json", JSON.stringify({ ok: true }), "utf8");
await setTimeout(10);
assert.strictEqual(fs.existsSync("result.json"), true);
```

## The execution model

The runtime data model is the same as in the rest of DBSP: every topic carries a Z-set, and in the
JavaScript API a Z-set is represented as an array of `[document, weight]` pairs. Positive weights
insert rows, negative weights remove rows, and updates are expressed as a delete plus an insert.

```js
publish("users", [
  [{ id: 1, name: "alice" }, 1],
  [{ id: 2, name: "bob" }, 1],
]);

publish("users", [
  [{ id: 1, name: "alice" }, -1],
  [{ id: 2, name: "bob" }, -1],
  [{ id: 2, name: "bob-updated" }, 1],
]);
```

This is enough to build small end-to-end experiments entirely in JavaScript. A typical script
defines input schemas or pipelines, compiles a circuit, transforms it with `Incrementalizer`, and then
publishes a few test deltas into input topics.

```js
const c = aggregate.compile([
  { "@select": { "@eq": ["$.metadata.namespace", "default"] } },
  { "@project": { name: "$.metadata.name", status: "$.status" } }
], { inputs: "pods", output: "result" });

c.transform("Incrementalizer");
publish("pods", [[{ metadata: { name: "pod-a", namespace: "default" }, status: "Running" }, 1]]);
```

## JavaScript environment

The environment is intentionally small. It exposes `console.log`, the SQL compiler, the aggregation
compiler, `publish`/`subscribe`, `format` codecs, Kubernetes connectors, circuit observers, runtime
observers, and cancellation. Most top-level APIs also exist under the `runtime` object, so
`publish(...)` and `runtime.publish(...)` are equivalent.

All pub/sub and connector functions take the **topic name as the first argument**.

## Reference

### `console.log(...args)`

`console.log` prints to standard output. Strings are printed as-is, while objects are rendered as
JSON when possible.

```js
console.log("starting");
console.log({ phase: "compiled", ok: true });
```

### `sql.table(name, ddlCols)`

`sql.table` registers a table in the in-memory relational catalog used by the SQL compiler.
`ddlCols` is a comma-separated SQL-like column list. If no primary key is marked, the first column
becomes the primary key.

```js
sql.table("users", "id INTEGER PRIMARY KEY, name TEXT, age INTEGER");
sql.table("orders", "id INTEGER PRIMARY KEY, user_id INTEGER, total REAL");
```

### `sql.compile(query, { output })`

`sql.compile` compiles a SQL query and returns a circuit handle. The `output` option is required.
It may be a string or an object with `name` and optional `logical` fields.

```js
sql.table("users", "id INTEGER PRIMARY KEY, name TEXT, age INTEGER");

const c = sql.compile(
  "SELECT name, age FROM users WHERE age > 25",
  { output: "senior-users" }
);

c.transform("Incrementalizer");

publish("users", [
  [{ id: 1, name: "alice", age: 30 }, 1],
  [{ id: 2, name: "bob", age: 22 }, 1],
]);
```

SQL joins work the same way:

```js
sql.table("users", "id INTEGER PRIMARY KEY, name TEXT");
sql.table("orders", "id INTEGER PRIMARY KEY, user_id INTEGER, total REAL");

sql.compile(
  "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
  { output: "user-orders" }
).transform("Incrementalizer");
```

### `aggregate.compile(pipeline, { inputs, output })`

`aggregate.compile` compiles a declarative pipeline into a circuit handle. For single-input cases,
`inputs` may be omitted and defaults to `input`. For multi-input cases, pass explicit bindings.

```js
const c = aggregate.compile([
  { "@project": { "$.": "$." } }
], {
  inputs: "obs-input",
  output: "obs-output",
});

c.transform("Incrementalizer");
```

For pipelines with multiple logical inputs, bind topic names explicitly:

```js
const pipeline = [
  [{ "@inputs": ["pods", "services"] },
   { "@join": { "@eq": ["$.pods.metadata.name", "$.services.metadata.name"] } },
   { "@project": { pod: "$.pods.metadata.name", svc: "$.services.metadata.name" } },
   { "@output": "output" }]
];

aggregate.compile(pipeline, {
  inputs: [
    { name: "pods-topic", logical: "pods" },
    { name: "svc-topic", logical: "services" }
  ],
  output: { name: "result-topic", logical: "output" }
}).transform("Incrementalizer");
```

### Circuit handles: `.transform(name[, opts])`, `.validate()`, `.observe(fn)`

Both `sql.compile(...)` and `aggregate.compile(...)` return a circuit handle.

#### Circuit Transforms

`.transform(name[, opts])` applies a circuit transformer in place and returns the same handle.

Supported transformer names are:

- `"Incrementalizer"`
- `"Rewriter"`
- `"Reconciler"`
- `"Regularizer"`

Optional transformer options:

- `"Rewriter"`: `{ rules: "Pre" | "Post" | "Default" }`
- `"Reconciler"`: `{ pairs: [["inputID", "outputID"], ...] }`

`"Regularizer"` rewrites each output as `sum -> group_by(primary-key, identity) -> lexmin`
to ensure deterministic one-row-per-key output deltas.

`"Reconciler"` adds circuitry to handle self-referential input/output pairs. Pairs can be either
explicit raw node IDs (`input_services`, `output_desired_services`) or plain topic names
(`services`, `desired-services`), or `"Reconciler"` can also auto-detect self-referential
input/output pairs by node ID stem (`input_x` with `output_x`). If no pair is found, the transform
is a no-op.

Example with explicit reconciler pair:

```js
c.transform("Reconciler", {
  pairs: [[
    "services",
    "desired-services"
  ]]
});
```

#### Miscellaneous handles

`.validate()` checks the circuit. `.observe(fn)` attaches a handle-scoped execution observer. Pass
`null` or `undefined` to clear it.

```js
const c = aggregate.compile([
  { "@project": { "$.": "$." } }
], { inputs: "obs-in", output: "obs-out" });

c.observe((e) => {
  console.log("node", e.node.id, "position", e.position);
});

c.transform("Incrementalizer");
```

Handle-scoped observation is usually the easiest way to inspect a single script-built circuit.

### `publish(topic, entries)` and `runtime.publish(topic, entries)`

These functions publish one Z-set delta batch to a named topic.

```js
publish("products", [
  [{ pid: 1, name: "Widget", price: 9.99 }, 1],
  [{ pid: 2, name: "Gadget", price: 24.99 }, 1],
]);
```

### `subscribe(topic, fn)`

`subscribe(topic, fn)` registers a **sink** callback on a topic. `fn` receives a
`[[document, weight], ...]` entries array on every delta batch. The return value of `fn` is
**ignored** — it is a pure observer. To forward data, call `publish(...)` explicitly inside the
callback.

```js
subscribe("joined-orders", (entries) => {
  for (const [doc, weight] of entries) {
    console.log(weight, doc);
  }
});

subscribe("my-topic", (entries) => {
  publish("my-topic-copy", entries);
  cancel();
});
```

Callbacks run on the VM event loop. Calling `cancel()` inside a callback stops that callback owner.

### `subscribe.once(topic)`

`subscribe.once(topic)` returns a `Promise` that resolves with the first `[[document, weight], ...]`
batch published to the topic. Use this when you need a single event in an async script.

```js
const first = await subscribe.once("output");
publish("mirror", first);
```

```js
const t0 = performance.now();
const batch = await subscribe.once("output");
publish("input", [[{ id: 1 }, 1]]);
console.log("elapsed", performance.now() - t0, "ms", batch);
```

The runtime pub/sub delivers events only to subscribers registered at publish time, so subscribe
before publishing when using `subscribe.once`.

### `format.jsonl(entries)`, `format.json(entries)`, `format.yaml(entries)`, `format.csv(entries)`, `format.auto(entries)`

The `format` object provides synchronous codec functions. Each function accepts a
`[[document, weight], ...]` entries array, parses the `"line"` field of each document using the
named codec, and returns a new entries array with the decoded fields merged in.

`format.auto` detects the encoding of each line heuristically (JSON, YAML, or CSV).

These functions are pure transforms — they have no side effects and do not interact with the
pub/sub bus. They are most useful inside producer callbacks, where the return value is automatically
published:

```js
// Parse each raw pod-log line as JSONL before publishing to "pod-logs".
kubernetes.log("pod-logs", { name: "my-pod", namespace: "default" },
  (entries) => format.jsonl(entries));
```

### `kubernetes.watch(topic, { gvk, namespace, labels }[, callback])`

Starts a Kubernetes watcher and publishes object deltas into `topic` on every change.

`gvk` may be written as `v1/Kind` for core resources or `group/version/kind` for CRDs.
`namespace` and `labels` are optional filters.

The optional `callback(entries)` has **producer semantics**: its return value is published to
`topic`. Returning `undefined` or `null` publishes an empty Z-set (the batch is not passed through
unchanged — use the callback to select, project, or enrich). If no callback is provided, the raw
watch entries are published directly.

```js
// Publish raw Pod deltas to "pods".
kubernetes.watch("pods", { gvk: "v1/Pod", namespace: "default" });

// Filter out pods without a specific annotation before publishing.
kubernetes.watch("annotated-pods", { gvk: "v1/Pod" }, (entries) => {
  return entries.filter(([doc, _w]) => {
    const anns = (doc.metadata && doc.metadata.annotations) || {};
    return anns["dbsp-sentinel"] === "true";
  });
});
```

This works for native Kubernetes resources and, when the runtime can discover them, Δ-controller
view resources as well.

### `kubernetes.list(topic, { gvk, namespace, labels }[, callback])`

Starts a Kubernetes state-of-the-world source.

Like `kubernetes.watch`, but on each watch event it publishes the full filtered list of objects as
one output batch instead of the incremental delta. Useful for naive snapshot reconciliation loops.

Callback semantics are the same as for `kubernetes.watch`.

```js
kubernetes.list("services-sotw", { gvk: "v1/Service", namespace: "default" });
```

### `kubernetes.log(topic, { name, namespace, container }[, callback])`

Starts a Kubernetes pod log stream. Each log line is published to `topic` as a document
`{ "line": "<raw text>" }` at weight `+1`.

The optional `callback(entries)` has **producer semantics**: its return value is published to
`topic`. This is the natural place to apply a format codec:

```js
// Stream raw log lines.
kubernetes.log("pod-logs-raw", { name: "my-pod", namespace: "default" });

// Parse each line as JSONL before publishing.
kubernetes.log("pod-logs", { name: "my-pod", namespace: "default" },
  (entries) => format.jsonl(entries));
```

The producer reconnects with exponential backoff on stream errors and returns cleanly when the
context is cancelled.

### `kubernetes.patch(topic, { gvk })`

Installs a Kubernetes sink that reads delta batches from `topic` and applies each document as a
strategic merge patch to the matching existing object.

```js
kubernetes.patch("desired-services", { gvk: "v1/Service" });
```

Use this when the pipeline output is a partial object that should update an existing resource.

### `kubernetes.update(topic, { gvk })`

Installs a Kubernetes sink that reads delta batches from `topic` and writes each document as a
full object replacement.

```js
kubernetes.update("deployments-full", { gvk: "apps/v1/Deployment" });
```

Use this when the pipeline emits the complete desired object rather than a patch.

### Callback semantics summary

| Context | What `fn` receives | Return value |
|---|---|---|
| `subscribe(topic, fn)` | `[[doc, weight], ...]` | **Ignored** — fn is a sink; call `publish(...)` to forward |
| `kubernetes.watch/list/log` callback | `[[doc, weight], ...]` | **Published** to the declared topic; `undefined`/`null` → empty Z-set |

### `runtime.onError(fn)`

`runtime.onError` installs a callback for asynchronous runtime errors. The callback receives an
object with `origin` and `message` fields.

```js
runtime.onError((e) => {
  console.error(`[runtime:${e.origin}] ${e.message}`);
});
```

If no error handler is installed, runtime errors are logged and the process exits.

### `runtime.observe(circuitName, fn)`

`runtime.observe` attaches an observer to a runtime circuit by name. This is useful when you want
to observe a circuit after validation or from code that only knows the runtime name.

```js
const c = aggregate.compile([
  { "@project": { "$.": "$." } }
], { inputs: "obs-input", output: "obs-output" });

runtime.observe("aggregation", (e) => {
  console.log("runtime observer", e.node.id, e.schedule);
  cancel();
});

publish("obs-input", [[{ id: 1, name: "alpha" }, 1]]);
```

Pass `null` or `undefined` as the second argument to remove the observer.

### `cancel()` and `runtime.cancel()`

`cancel()` is context-sensitive.

At top level it stops the script VM. Inside `subscribe`, `circuit.observe`, or
`runtime.observe`, it stops the current callback owner instead.

```js
subscribe("obs-output", (entries) => {
  console.log(entries);
  cancel();
});
```

### `performance.now()`

Returns high-resolution elapsed milliseconds since script start:

```js
const t0 = performance.now();
const batch = await subscribe.once("output");
publish("input", [[{ id: 1 }, 1]]);
await batch;
console.log(performance.now() - t0);
```

### `runtime.sql`, `runtime.aggregate`, and `runtime.subscribe`

The `runtime` object mirrors the main top-level APIs. This is useful when you want all scripting
operations to hang off a single namespace.

```js
runtime.sql.table("users", "id INTEGER PRIMARY KEY, name TEXT");
runtime.subscribe("users-out", (entries) => {
  console.log(entries);
  cancel();
});
```

## End-to-end example: pod log filter

The following script watches Pods by label selector, streams their logs, parses each line as JSONL,
and prints any document whose `level` field equals `"error"`.

```js
// Usage: dbsp pod-log-filter.js app:my-app

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

// Watch Pods matching the label selector.
kubernetes.watch("pods-raw", { gvk: "v1/Pod", labels });

// Project to pod identity and deduplicate.
aggregate.compile([
    { "@project": { "name": "$.metadata.name", "namespace": "$.metadata.namespace" } },
    { "@distinct": {} },
], { inputs: "pods-raw", output: "pods" }).transform("Incrementalizer").validate();

// Shared filter circuit: keep only error-level documents.
aggregate.compile([
    { "@select": { "@eq": ["$.level", "error"] } },
], { inputs: "pod-logs-raw", output: "error-logs" }).transform("Incrementalizer").validate();

// For each new pod, start a log stream with JSONL parsing.
subscribe("pods", (entries) => {
    for (const [pod, weight] of entries) {
        if (weight > 0) {
            kubernetes.log("pod-logs-raw", {
                name:      pod.name,
                namespace: pod.namespace || "default",
            }, (entries) => format.jsonl(entries));
        }
    }
});

// Print every error-level document.
subscribe("error-logs", (entries) => {
    for (const [doc, weight] of entries) {
        if (weight > 0) console.log("[ERROR]", JSON.stringify(doc));
    }
});
```

## Current limitations

The CLI requires a script path today, because the interactive REPL is not implemented yet.

If no kubeconfig is available, native Kubernetes resources are disabled, though view resources can
still be used when the runtime can resolve them.
