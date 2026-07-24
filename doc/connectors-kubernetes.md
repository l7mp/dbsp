# The Kubernetes connector

A connector is the boundary between a DBSP pipeline and the outside world. It has two halves:
producers, which turn external state into Z-set events on an input topic, and consumers, which turn
output topic events back into external writes. The Kubernetes connector implements both halves
against an apiserver: it watches or lists objects into topics, and applies topic documents back as
patches, updates, or a whole-kind snapshot.

Nothing in the connector knows about circuits. To a pipeline a watch is just a topic that receives
Z-sets, exactly like a `publish()` call, which is what makes the same pipeline runnable against a
live cluster, a test fixture, or a synthetic load generator.

## Initialization

One call sets up the shared client and starts the connector runtime:

```js
kubernetes.runtime.start();
```

With no arguments this uses the in-cluster service account when running inside a pod, and the
ambient kubeconfig otherwise. Pass an explicit one when needed:

```js
kubernetes.runtime.start({ kubeconfig: "/home/me/.kube/config" });
```

Start the runtime before installing any producer or consumer. Objects are addressed by a GVK string
throughout, `"v1/Service"` for core kinds and `"apps/v1/Deployment"` for grouped ones.

## Ingest: Watcher and Lister

Both producers follow the same shape, `kubernetes.<verb>(topic, options[, callback])`, and differ
only in what they emit.

**Watcher** (`kubernetes.watch`) is the incremental source: it emits the delta of every observed
change. An added object arrives with weight +1, a deleted one with weight -1, and a modification as
both, -1 for the old version and +1 for the new. This is the natural input of an incremental
pipeline.

```js
kubernetes.watch("services", {
  gvk:       "v1/Service",
  namespace: "default",
  labels:    { app: "frontend" },
});
```

`namespace` and `labels` are optional; without them the watch is cluster-wide and unfiltered.

**Lister** (`kubernetes.list`) is the state-of-the-world source: on every change it emits a full
snapshot of the matching objects rather than the change itself. It takes the same options, and is
what a non-incremental (snapshot) pipeline consumes, mostly for comparison and benchmarking.

```js
kubernetes.list("services", { gvk: "v1/Service", namespace: "default" });
```

Either producer accepts an optional callback with producer semantics: it receives the entries about
to be published and its return value is what actually gets published, so it doubles as a tap or a
filter.

```js
kubernetes.watch("services", { gvk: "v1/Service" }, (entries) => {
  logger.info({ count: entries.length }, "observed services");
  return entries;
});
```

## Egress: Patcher, Updater, and Setter

The three consumers all bind an output topic to a target kind, `kubernetes.<verb>(topic, {gvk}[,
callback])`, and differ in how much of the object they claim to own. In all three a positive weight
means apply and a negative weight means delete.

**Patcher** (`kubernetes.patch`) applies each document as an RFC 7386 merge patch. Only the fields
present in the document are touched, so the pipeline can own an annotation or a status condition
while leaving everything else on the object to its other owners. This is the usual choice for
writing into objects the controller does not own outright.

```js
kubernetes.patch("desired-services", { gvk: "v1/Service" });
```

**Updater** (`kubernetes.update`) replaces the whole object with the emitted document. The pipeline
must therefore emit the complete object, and anything it omits is dropped.

```js
kubernetes.update("desired-services", { gvk: "v1/Service" });
```

**Setter** (`kubernetes.set`) is the state-of-the-world write target: each event carries the
complete desired population of the target kind, and the Setter reconciles the cluster to it against
a fresh List. Objects in the event are created or updated, an update being skipped when the current
content already matches, and objects of that kind absent from the event are deleted.

```js
kubernetes.set("desired-services", { gvk: "v1/Service" });
```

Because deletion is by omission, the Setter owns the entire target kind. Point it only at kinds
whose full population the controller means to own; placement (which namespaces, which labels) is the
pipeline's business alone, since an object emitted outside the intended scope would be created by
the Setter but invisible to its List, and so never cleaned up.

Like the producers, every consumer accepts an optional callback that sees the entries before they
are applied.

## Which one to use

| Role | Delta | Snapshot |
|---|---|---|
| Ingest | `kubernetes.watch` | `kubernetes.list` |
| Egress, partial ownership | `kubernetes.patch` | |
| Egress, whole object | `kubernetes.update` | |
| Egress, whole kind | | `kubernetes.set` |

A delta producer paired with a delta consumer is the incremental configuration, and the one the
engine is built for. The snapshot pair exists so that the same pipeline can be run the traditional
way and the two compared.

## A minimal controller

Watch in, transform, patch out:

```js
kubernetes.runtime.start();

kubernetes.watch("services", { gvk: "v1/Service", namespace: "default" });

aggregate.compile([
  { "@select": { "@eq": ["$.metadata.name", "iperf-server"] } },
  { "@project": {
      metadata: {
        name:        "$.metadata.name",
        namespace:   "$.metadata.namespace",
        annotations: { "dbsp-sentinel": "true" },
      },
  }},
], { inputs: ["services"], outputs: ["desired-services"] })
  .transform([{ name: "Incrementalizer" }])
  .commit();

kubernetes.patch("desired-services", { gvk: "v1/Service" });
```

The pipeline states what the annotation should be; the connector works out the writes.
