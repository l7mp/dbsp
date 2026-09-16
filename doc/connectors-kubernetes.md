# The Kubernetes connector

A connector is the boundary between a DBSP pipeline and the outside world. It has two halves:
producers, which turn external state into Z-set events on an input topic, and consumers, which turn
output topic events back into external writes. The Kubernetes connector implements both halves
against an apiserver: it watches objects into topics, and applies topic documents back as patches
or updates.

Nothing in the connector knows about circuits, and every stream carries deltas: an added object
arrives with weight +1, a deleted one with weight -1, a modification as both. To a pipeline a
watch is just a topic that receives Z-sets, exactly like a `publish()` call, which is what makes
the same pipeline runnable against a live cluster, a test fixture, or a synthetic load generator.
State-of-the-world computation is not the connector's business either: a snapshot circuit
reconstructs full state with its own input integrators (see the
[transforms guide](/doc/concepts-transforms.md)), fed by the same delta watch.

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

The Kubernetes runtime is a singleton owned by the host: one per process, shared by every DBSP
runtime that binds Kubernetes resources. Start it before installing any producer or consumer.
Objects are addressed by a GVK string throughout, `"v1/Service"` for core kinds and
`"apps/v1/Deployment"` for grouped ones.

## Ingest: the Watcher

`kubernetes.watch(topic, options[, callback])` emits the delta of every observed change. An added
object arrives with weight +1, a deleted one with weight -1, and a modification as both, -1 for
the old version and +1 for the new. The watcher survives apiserver watch expiry: it reconnects
with a resume cursor and reports loudly when the cursor is lost.

```js
kubernetes.watch("services", {
  gvk:       "v1/Service",
  namespace: "default",
  labels:    { app: "frontend" },
});
```

`namespace`, `labels`/`labelSelector` and `predicate` are optional; without them the watch is
cluster-wide and unfiltered.

The optional callback has producer semantics: it receives the entries about to be published and
its return value is what actually gets published, so it doubles as a tap or a filter.

```js
kubernetes.watch("services", { gvk: "v1/Service" }, (entries) => {
  logger.info({ count: entries.length }, "observed services");
  return entries;
});
```

## Egress: Patcher and Updater

The two consumers bind an output topic to a target kind, `kubernetes.<verb>(topic, {gvk})`, and
differ only in the ownership model: Updater can create, modify or delete an object, while Patcher
can only ever modify the specified fields of an *existing* object.

**Patcher** (`kubernetes.patch`) applies each (old, new) pair as an RFC 7386 merge patch.
Only the fields present in the document are touched, so the pipeline can own an annotation or a
status condition while leaving everything else on the object to its other owners. A patcher never
creates or deletes an object: a decorator has no business bringing an object back.

```js
kubernetes.patch("desired-services", { gvk: "v1/Service" });
```

**Updater** (`kubernetes.update`) owns the objects it writes. Updates are the same merge patch the
Patcher sends; ownership shows at the edges: a bare assertion creates the object, a bare
retraction deletes it, and an update whose target is gone recreates it.

```js
kubernetes.update("desired-services", { gvk: "apps/v1/Deployment" });
```

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
