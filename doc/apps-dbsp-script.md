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
compiler, producer and consumer helpers, circuit observers, runtime observers, cancellation, and
Kubernetes connectors. Most top-level APIs also exist under the `runtime` object, so
`publish(...)` and `runtime.publish(...)` are equivalent.

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

### `producer(topic[, entries])` and `producer.publish(entries)`

`producer` creates a publishing handle for a topic. If `entries` is passed immediately, the handle
publishes them once during construction.

```js
const p = producer("orders");
p.publish([
  [{ oid: 101, product_id: 1, qty: 3 }, 1],
  [{ oid: 102, product_id: 2, qty: 1 }, 1],
]);
```

For one-off publishing, use the shorthand `publish(topic, entries)`.

```js
publish("products", [
  [{ pid: 1, name: "Widget", price: 9.99 }, 1],
  [{ pid: 2, name: "Gadget", price: 24.99 }, 1],
]);
```

### `consumer(topic, fn)` and `subscribe(topic, fn)`

`consumer` registers a callback that receives one batch of Z-set entries per runtime event on the
given topic. `subscribe` is the shorthand form and behaves the same way.

```js
consumer("joined-orders", (entries) => {
  for (const [doc, weight] of entries) {
    console.log(weight, doc);
  }
});

subscribe("my-topic", (entries) => {
  runtime.publish("my-topic-copy", entries);
  cancel();
});
```

Callbacks run on the VM event loop. Inside a callback, `cancel()` stops that callback owner.

### `publish(topic, entries)` and `runtime.publish(topic, entries)`

These functions publish one Z-set delta batch to a topic without creating an explicit producer
handle.

```js
runtime.publish("audit", [[{ action: "compiled" }, 1]]);
publish("users", [[{ id: 7, name: "carol" }, 1]]);
```

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

`runtime.observe` attaches an observer to a runtime circuit by name. This is useful when you want to observe a circuit after validation or from code that only knows the runtime name.

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

At top level it stops the script VM. Inside `consumer`, `subscribe`, `circuit.observe`, or
`runtime.observe`, it stops the current callback owner instead.

```js
consumer("obs-output", (entries) => {
  console.log(entries);
  cancel();
});
```

### `runtime.sql`, `runtime.aggregate`, `runtime.producer`, `runtime.consumer`, and `runtime.subscribe`

The `runtime` object mirrors the main top-level APIs. This is useful when you want all scripting
operations to hang off a single namespace.

```js
runtime.sql.table("users", "id INTEGER PRIMARY KEY, name TEXT");
runtime.subscribe("users-out", (entries) => {
  console.log(entries);
  cancel();
});
```

### `producer.kubernetes.watch({ gvk, namespace, labels, topic }[, fn])`

This starts a Kubernetes watcher and publishes object deltas into a runtime topic.

`gvk` may be written as `v1/Kind` for core resources or `group/version/kind` for grouped APIs.
`namespace` and `labels` are optional filters.

An optional callback `fn(entries)` can transform each watched batch before it is published:

- return a new entries array to publish transformed output;
- return `undefined` to pass through the original entries unchanged;
- return `null` to drop the batch.

If provided, `fn` must be a function.

```js
producer.kubernetes.watch({
  gvk: "v1/Service",
  namespace: "default",
  topic: "services",
});

producer.kubernetes.watch({
  gvk: "v1/Service",
  namespace: "default",
  topic: "services-with-label",
}, (entries) => {
  const out = [];
  for (const [doc, weight] of entries) {
    const anns = (doc.metadata && doc.metadata.annotations) || {};
    if (anns["dbsp-sentinel"] !== "true") {
      out.push([doc, weight]);
    }
  }
  return out;
});
```

This works for native Kubernetes resources and, when the runtime can discover them, Δ-controller
view resources as well.

### `producer.kubernetes.list({ gvk, namespace, labels, topic }[, fn])`

This starts a Kubernetes state-of-the-world source.

It subscribes to watch events as a trigger, but each trigger publishes the full filtered list of
objects as one output batch. This is useful for naive snapshot reconciliation loops.

Filtering and optional callback semantics are the same as for `producer.kubernetes.watch(...)`.

```js
producer.kubernetes.list({
  gvk: "v1/Service",
  namespace: "default",
  topic: "services-sotw",
});
```

### `consumer.kubernetes.patcher({ gvk, topic }[, fn])`

This installs a Kubernetes consumer that applies merge-style patches from a topic onto existing
objects.

An optional callback `fn(entries)` can transform each batch before it is applied:

- return a new entries array to apply transformed output;
- return `undefined` to pass through the original entries unchanged;
- return `null` to drop the batch.

```js
consumer.kubernetes.patcher({
  gvk: "v1/Service",
  topic: "desired-services",
});

consumer.kubernetes.patcher({
  gvk: "v1/Service",
  topic: "desired-services",
}, (entries) => {
  return entries.filter(([doc, _w]) => doc?.metadata?.name !== "kubernetes");
});
```

Use this when the pipeline output is a partial object that should update an existing resource.

### `consumer.kubernetes.updater({ gvk, topic }[, fn])`

This installs a Kubernetes consumer that writes whole objects from a topic.

Optional callback semantics are the same as for `consumer.kubernetes.patcher(...)`.

```js
consumer.kubernetes.updater({
  gvk: "apps/v1/Deployment",
  topic: "deployments-full",
});
```

Use this when the pipeline emits the full desired object rather than a patch.

In short: plain `consumer(...)` / `subscribe(...)` callbacks are sink observers (return value is
ignored), while connector callbacks (`producer.kubernetes.*`, `consumer.kubernetes.*`) are
transform callbacks (return value controls the forwarded batch).

### `producer.jsonl(...)` and `consumer.redis(...)`

These entry points exist today but are placeholders. In the current implementation,
`producer.jsonl(...)` and `consumer.redis(...)` return a not-implemented error.

## Current limitations

The CLI requires a script path today, because the interactive REPL is not implemented yet.

If no kubeconfig is available, native Kubernetes resources are disabled, though view resources can
still be used when the runtime can resolve them.
