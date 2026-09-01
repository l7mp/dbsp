# Sources, Circuits, and Targets

An `Operator` custom resource is a frozen DBSP runtime: its spec is the engine's serialized
runtime format verbatim, and Δ-controller assembles one private runtime per operator from it. The
spec mirrors what a runtime is, three sets coupled by streams:

```yaml
spec:
  sources:   # bindings feeding streams
  circuits:  # programs over streams
  targets:   # bindings consuming streams
```

## Streams

Streams couple the sets by name. A source feeds the stream its `as` names, defaulting to the
resource kind; a target consumes likewise; a circuit lists its input and output streams. Every
stream is one topic named plainly by the stream, and every stream carries deltas.

A stream produced and consumed only by circuits is an internal wire with no binding at all:
that is how circuits chain. A kind that is both read and written in one runtime must be
disambiguated with `as` on one side (a stream fed by a source and consumed by a target would
short-circuit the pair around every circuit, and assembly refuses the collision):

```yaml
sources:
  - apiGroup: ""
    kind: Service
targets:
  - apiGroup: ""
    kind: Service
    type: Patcher
    as: ServiceAnnotation   # the output stream, distinct from the watched "Service"
```

## Views

A view is an in-memory, unstructured object type owned by the operator, behaving like a Kubernetes
resource but stored only in the manager's memory. The view API group is derived from the operator
name, `<operator-name>.view.dcontroller.io/v1alpha1`, and view objects need valid `metadata.name`
and `metadata.namespace`. Omitting `apiGroup` on a binding means the operator's own view group,
while `apiGroup: ""` means the core Kubernetes group; views are inspectable through the embedded
API server. Use a view target/source pair when the intermediate state should be visible to
kubectl or to other operators; use an internal stream when it should not.

## Sources

A source always has a `kind`, and may specify `apiGroup`, `version`, `as`, filters, and a
connector-specific type. Kubernetes sources are Watchers, the delta ingest:

```yaml
sources:
  - apiGroup: ""
    kind: Pod
    namespace: default
    labelSelector:
      matchLabels:
        app: web
    predicate: GenerationChanged   # optional event filter
```

The misc connector provides synthetic trigger sources under its own group, kind `Timer`: `Tick`
emits a trigger document every `parameters.period` (retracting the previous one), `Init` emits a
single trigger at startup.

```yaml
sources:
  - apiGroup: misc.connector.dcontroller.io
    kind: Timer
    type: Tick
    as: Resync
    parameters:
      name: resync
      period: 30s
```

## Targets

A target binds an output stream to a written resource. There are two modes, and neither ever reads
the cluster: writes accumulate and retry until the apiserver accepts them.

`Updater` (the default) owns the objects it writes: a bare assertion creates the object, a bare
retraction deletes it. It is the natural target for views and for whole generated objects.
`Patcher` decorates somebody else's objects with an RFC 7386 merge patch: only the emitted fields
are touched, and a patcher never creates or deletes. It is the safe choice for annotations and
status fields on native resources.

## Circuits

A circuit is a program over the runtime's streams, with its transform chain:

```yaml
circuits:
  - name: pod-health
    inputs: [Pod]           # default: the single source stream
    outputs: [HealthView]   # default: the single target stream
    pipeline:
      - "@project": { ... }
    transforms:
      - name: Reconciler
      - name: Distincter
      - name: Incrementalizer
```

The program is exactly one of `pipeline` (an aggregation pipeline), `sql`, or `graph` (a
hand-built circuit). `inputs`/`outputs` may be omitted only when the runtime has a single source
or target stream to default to.

Transforms convert the circuit. The transform chain states what the circuit is; the engine applies it
in canonical order (see the [transforms guide](/doc/concepts-transforms.md)). An empty chain means
snapshot execution: the engine compiles the program as ∫ -> Q -> D, recomputing over the full
state while the streams stay deltas.

With a single source, a pipeline is a short stage sequence:

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

With multiple input streams, the pipeline starts with `@join` and refers to each stream by its
name:

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

## A complete operator

```yaml
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: example-operator
spec:
  sources:
    - apiGroup: ""
      kind: Service
  circuits:
    - name: annotate-service
      pipeline:
        - "@project":
            metadata:
              name: "$.metadata.name"
              namespace: "$.metadata.namespace"
              annotations:
                "example.io/managed": "true"
      transforms:
        - name: Reconciler
        - name: Distincter
        - name: Incrementalizer
  targets:
    - apiGroup: ""
      kind: Service
      type: Patcher
      as: ServiceAnnotation
```

When the operator is running, inspect `status.conditions` and `status.lastErrors` on the
`Operator` object to see whether the configuration was accepted and started successfully; runtime
errors of an operator are routed per operator into its status.
