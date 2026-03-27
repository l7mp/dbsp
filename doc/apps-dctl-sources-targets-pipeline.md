# Sources, Targets, and Pipelines

At runtime, a Δ-controller controller is a small dataflow graph. It receives deltas from its
sources, applies a declarative pipeline, and emits objects to its targets.

## Operators and views

An `Operator` custom resource is a container for one or more controllers. Controllers in the same
operator can communicate through local views. A view is an in-memory, unstructured object type
owned by that operator. It behaves like a Kubernetes resource from the pipeline's point of view,
but it is not stored in the main API server.

The view API group is derived from the operator name:

```text
<operator-name>.view.dcontroller.io/v1alpha1
```

Every view object must still have valid `metadata.name` and `metadata.namespace`, because those
fields are used as the object key inside the runtime and by the embedded API server.

## Sources

A source defines where a controller reads from. A source always has a `kind`, and may also specify
`apiGroup`, `version`, filters, and a source type.

For native Kubernetes resources, use the real API group. For core resources such as `Pod` or
`Service`, use `apiGroup: ""`.

```yaml
sources:
  - apiGroup: ""
    kind: Service
  - apiGroup: discovery.k8s.io
    kind: EndpointSlice
```

For local views, omit `apiGroup` entirely:

```yaml
sources:
  - kind: HealthView
```

This distinction is important in the current implementation: omitted `apiGroup` means local view,
while `apiGroup: ""` means the core Kubernetes API group.

Watcher sources may also be restricted with `namespace`, `labelSelector`, or `predicate`.

```yaml
sources:
  - apiGroup: ""
    kind: Pod
    namespace: default
    labelSelector:
      matchLabels:
        app: web
```

For state-of-the-world reconciliation, use a `Lister` source. A lister still subscribes to watch
events, but each trigger emits the full filtered list as the input batch.

```yaml
sources:
  - apiGroup: ""
    kind: Service
    type: Lister
    namespace: default
```

Δ-controller also supports two synthetic source types.

`OneShot` emits one empty trigger object when the controller starts. `Periodic` emits trigger
objects on a timer.

```yaml
sources:
  - kind: Tick
    type: Periodic
    parameters:
      period: 30s
```

## Targets

A target defines where pipeline output is written. In the current API a controller has `targets`,
not `target`, even if there is only one output.

```yaml
targets:
  - apiGroup: ""
    kind: Service
    type: Patcher
```

As with sources, omit `apiGroup` when the target is a local view:

```yaml
targets:
  - kind: HealthView
```

There are two target modes.

`Updater` is the default. It is appropriate when the pipeline produces the whole desired object,
especially for local views and simple resources. `Patcher` merges the pipeline output onto an
existing object and is the safer choice for modifying complex native resources such as `Deployment`
or `Service`.

## Pipelines

Pipelines are the declarative part of the controller: it is basically an "aggregation" pipeline
that processes Kubernetes objects produce by the Source(s) and feeding the results into the
Target(s). It is also possible to run raw circuits instead of a pipeline; use the `circuit:
<cirruit>` form to do that.

With a single source, a pipeline is usually a short sequence of transformation stages:

```yaml
pipeline:
  - "@select":
      "@exists": '$["metadata"]["annotations"]["example.io/enabled"]'
  - "@project":
      metadata:
        name: "$.metadata.name"
        namespace: "$.metadata.namespace"
      spec:
        enabled: true
```

With multiple sources, the pipeline starts with `@join`, then reshapes the joined object.

```yaml
pipeline:
  - "@join":
      "@and":
        - "@eq": ["$.HealthView.metadata.name", "$.Service.metadata.name"]
        - "@eq": ["$.HealthView.metadata.namespace", "$.Service.metadata.namespace"]
  - "@project":
      metadata:
        name: "$.Service.metadata.name"
        namespace: "$.Service.metadata.namespace"
```

The full expression language is documented in the generic reference guides. On the Δ-controller
side, the important point is simply that the pipeline describes the snapshot transformation while
the runtime executes it incrementally.

## Operators

The current operator shape is:

```yaml
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: example-operator
spec:
  controllers:
    - name: annotate-service
      sources:
        - apiGroup: ""
          kind: Service
      pipeline:
        - "@project":
            metadata:
              name: "$.metadata.name"
              namespace: "$.metadata.namespace"
              annotations:
                "example.io/managed": "true"
      targets:
        - apiGroup: ""
          kind: Service
          type: Patcher
```

When the operator is running, inspect `status.conditions` and `status.lastErrors` on the top-level
`Operator` object to see whether the configuration was accepted and started successfully.
