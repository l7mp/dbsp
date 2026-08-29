# Reference: Aggregation Pipelines

Aggregation pipelines are the preferred declarative query language used by the DBSP aggregation
compiler.

An aggregation pipeline is an ordered list of stages. Each stage reads the stream produced by the
previous stage, applies a DBSP operator to it, and emits a new stream. In the single-input case,
this feels like a document transformation pipeline. In the multi-input case, the first stage is
usually a join that turns several input streams into one combined stream.

## Pipelines

A pipeline can be written in three useful forms.

The shortest form is a single stage:

```yaml
pipeline:
  "@project":
    metadata:
      name: "$.metadata.name"
```

The common form is a list of stages, each an operator plus arguments:

```yaml
pipeline:
  - "@select":
      "@eq": ["$.metadata.namespace", "prod"]
  - "@project":
      metadata:
        name: "$.metadata.name"
```

The advanced form is a list of branches. This is mainly used by the aggregation compiler directly,
not by most Δ-controller examples.

```yaml
[
  [
    {"@inputs": ["pods"]},
    {"@project": {"$.metadata.name": "a"}},
    {"@output": "branch1"}
  ],
  [
    {"@inputs": ["branch1"]},
    {"@project": {"$.metadata.namespace": "default"}},
    {"@output": "final"}
  ]
]
```

The important rule is that the branch dependency graph must be acyclic. A branch may depend on an
earlier branch output, but cyclic branch wiring is rejected.

## Inputs, outputs, and branches

The compiler supports two directive stages that are only needed in explicit multi-branch programs.

`@inputs` selects which named logical streams a branch reads from.

```yaml
{"@inputs": ["pods", "services"]}
```

If you are compiling a single pipeline with one configured input, you normally do not need this at
all. It becomes useful when you explicitly wire several branches together.

`@output` names the logical output stream produced by a branch.

```yaml
{"@output": "service-view"}
```

Again, in the simple single-branch case this is usually implied by the configured compiler output.

## Multi-source pipelines: `@join`

The `@join` operator is the only operator that can combine several input streams into a single
stream. In the current compiler, if a branch reads from more than one input, `@join` must be the
first non-directive stage.

```yaml
pipeline:
  - "@join":
      "@eq": ["$.dep.metadata.name", "$.pod.spec.parent"]
  - "@project":
      metadata:
        name: result
        namespace: default
      pod: "$.pod"
      dep: "$.dep"
```

The join predicate is evaluated on a compound document whose top-level fields are the logical input
names.

```yaml
pod:
  metadata:
    name: pod-1
  spec:
    parent: dep-1
dep:
  metadata:
    name: dep-1
```

The join operator conceptually takes the Cartesian product of the inputs and keeps only the pairs
or tuples whose predicate evaluates to `true`. With two inputs `@join` behaves like an inner join,
and with three or more inputs the predicate usually becomes an `@and` of several equality checks.

```yaml
"@join":
  "@and":
    - {"@eq": ["$.dep.metadata.name", "$.pod.spec.parent"]}
    - {"@eq": ["$.dep.metadata.name", "$.rs.spec.dep"]}
```

That reads naturally as: join a deployment, pod, and ReplicaSet when they all belong to the same
deployment name.

The two-element form `["@join": [predicate, options]]` takes an options object next to the
predicate.

The `soft` option turns the named inputs into left-join sides: rows of the other (hard) inputs are
kept even when no partner matches, with the soft input's namespace set to `null` (test it with
`@isnull`, or default fields with `@definedOr`).

```yaml
"@join":
  - {"@eq": ["$.listeners.gateway", "$.counts.key"]}
  - soft: [counts]
```

The `index` option makes a join indexed: it names exactly two participants and gives each a
join-key expression, evaluated against that input's own document (so `$.` is the Service, the
Gateway, not the compound join document). The pair is joined by key equality through a hash index
instead of filtering the full Cartesian product, and after incrementalization the index is
persistent: a delta on either side only touches the rows with the same key, instead of re-scanning
the whole other side. This is usually much faster than the non-indexed form; otherwise, indexed and
the non-indexed results are equivalent.

Semantically the index adds a key-equality condition `AND`-ed with the predicate, so the equality
it expresses can be dropped from the predicate. In the example below the gw-gwc equality lives
entirely in the index and the predicate carries only the residual controller-name filter (use
`true` as the predicate when nothing is left):

```yaml
"@join":
  - {"@eq": ["$.gwc.spec.controllerName", "example.com/our-controller"]}
  - index:
      gw: "$.spec.gatewayClassName"      # gw.spec.gatewayClassName ==
      gwc: "$.metadata.name"             #   gwc.metadata.name
```
 Keys may be arbitrary expressions, including
composite objects: two keys are equal when their canonical serializations are equal, so both sides
should build the same shape:

```yaml
index:
  backends: {name: "$.backendService.name", namespace: "$.backendService.namespace"}
  svcports: {name: "$.service.name", namespace: "$.service.namespace"}
```

`index` composes with `soft` (the indexed pair may be the hard/soft pair of a left join), and with
three or more participants it accelerates the one join site where the two named inputs meet; the
remaining sites stay Cartesian. Rows whose key expression does not resolve simply never match,
mirroring the field-not-found-is-false convention of predicates.

## Filtering: `@select`

The `@select` operator keeps only the documents whose predicate evaluates to `true`. The surviving
documents pass through unchanged.

```yaml
"@select":
  "@gt": ["$.spec.replicas", 3]
```

This means "let only large deployments through". If the input object is:

```yaml
metadata:
  name: web
spec:
  replicas: 5
```

then the document survives. If `replicas` is `2`, it is dropped.

Note that if a field is missing during `@select`, the compiler treats that predicate result as
`false`, so the document is simply filtered out.

## Reshaping: `@project`

The `@project` operator is the main shape-changing operator. It takes the current document and
produces a new one.

There are two modes: object construction and sequential projection.

The most common form contains only a single projection:

```yaml
"@project":
  metadata:
    name: "$.name"
    namespace: "$.namespace"
  node: "$.nodeName"
```

If the input is:

```yaml
name: pod-a
namespace: default
nodeName: node-1
restartPolicy: Always
```

then the output is:

```yaml
metadata:
  name: pod-a
  namespace: default
node: node-1
```

The important rule is that `@project` does not preserve fields automatically. If you do not copy a
field, it is gone from the result.

The other form is a list of projections applied in order.

```yaml
"@project":
  - {"$.": "$."}
  - {"$.metadata.name": "fixed"}
  - {"$.spec.done": true}
```

This starts from a full copy of the input document, then overrides or adds a few fields. This is
often easier to read than rebuilding a large object from scratch when most of the original document
should stay intact.

## Expanding lists: `@unwind`

The `@unwind` operator takes one document containing a list field and emits one output document per
list item.

```yaml
"@unwind": "$.spec.ports"
```

If the input is:

```yaml
metadata:
  name: my-svc
spec:
  ports:
    - {name: http, port: 80}
    - {name: https, port: 443}
```

then the operator emits two documents. In each output document, `spec.ports` is replaced by one
single port object. Nothing else is touched: the operator injects no bookkeeping and rewrites no
fields. 

Note that equal rows merge: When two output documents come out identical (duplicate elements in one
list, or two inputs that differ only inside the unwound field), their weights add up into a single
Z-set entry. Note also that the original list order is lost after `@unwind`: DBSP's internal Z-sets
are unordered, so once a list is unwound the original element positions are unrecoverable, unless
they were captured in the documents first. Pair the list with
[`@enumerate`](reference-expressions.md#enumerate) before unwinding; the index then travels with
each row:

```yaml
[
  {"@project": [{"$.": "$."}, {rules: {"@enumerate": ["$.spec.rules"]}}]},
  {"@unwind": "$.rules"}
]
```

In the result each output document carries `rules.index` and `rules.value`. The index doubles as a
row discriminator (duplicate elements stay distinct) and lets a later stage rebuild the list in its
original order:

```yaml
[
  {"@groupBy": ["$.someKey", "$.rules"]},
  {"@project": {key: "$.key",
                rules: {"@map": ["$$.value",
                                 {"@sortBy": [{"@switch": [
                                     [{"@lt": ["$$.a.index", "$$.b.index"]}, -1],
                                     [{"@eq": ["$$.a.index", "$$.b.index"]}, 0],
                                     [true, 1]]},
                                   "$.values"]}]}}}
]
```

Nested unwind is also allowed:

```yaml
[
  {"@unwind": "$.endpoints"},
  {"@unwind": "$.endpoints.addresses"}
]
```

This first expands the top-level `endpoints` list, then expands the `addresses` list inside each
endpoint.

## Deduplication: `@distinct`

The `@distinct` operator converts a Z-set into set membership. It takes no argument, so the bare
string form is the natural spelling; `{"@distinct": null}` is the equivalent explicit form.

```yaml
"@distinct": {}
```

For each document hash, `@distinct` emits weight `1` when the accumulated multiplicity is positive,
and emits nothing otherwise. This is useful after projection when several inputs can map to the same
output object.

## Grouping: `@groupBy`

The `@groupBy` operator is the inverse pattern of `@unwind`: many input documents become grouped
summary documents.

Its argument is:

```yaml
"@groupBy": [<keyExpr>, <valueExpr>]
```

or:

```yaml
"@groupBy": [<keyExpr>, <valueExpr>, {distinct: true}]
```

The key expression is required (`null` is a compile error); the value expression may be `null`, in
which case the whole document is collected. With `{distinct: true}`, duplicate values are collapsed.

The output shape of `@groupBy` in the current implementation is a document with three fields:
- `key`: the grouping key,
- `values`: the collected values,
- `documents`: the original input documents that contributed to the group.

A basic example:

```yaml
"@groupBy": ["$.metadata.namespace", "$.spec.a"]
```

If the input stream contains:

```yaml
{metadata: {name: a, namespace: default}, spec: {a: 1}}
{metadata: {name: b, namespace: default}, spec: {a: 2}}
```

then the output is one grouped document like:

```yaml
key: default
values: [1, 2]
documents:
  - {metadata: {name: a, namespace: default}, spec: {a: 1}}
  - {metadata: {name: b, namespace: default}, spec: {a: 2}}
```

## Time-variant fields: `@stamp`

Some fields cannot be generated from the input: a Kubernetes condition's `lastTransitionTime`, a
generated name or port, a random pick, etc. For instance, writing `{"@now": null}` into a
`@project` gets the object restamped every time it is recomputed, and in an incremental circuit
when the object is retracted at a later clock it never cancels the object that was inserted since
they hold different timestamps. Such fields are called "time-variant" in DBSP, and they are
generally cannot be incrementalized with the default DBSP engine.

The `@stamp` operator embeds a special machinery to handle such time-variant fields:

```yaml
- "@stamp": [keyExpr, {"$.path.to.field": valueExpr, ...}]
```

Here, `keyExpr` identifies the key to the time-variant field, and `$.path.to.field` is the JSONPath
and `valueExpr` is the expression to be used for setting the field. Then, `@stamp` will handle all
internal complexity of incrementalizing the time-variant field. 

For instance, Kubernetes status conditions require the `lastTransitionTime` to hold the time the
status changed last. This can be handled with `@stamp` as follows:

```yaml
- "@unwind": "$.status.conditions"
- "@stamp":
    - ["$.metadata.namespace", "$.metadata.name",
       "$.status.conditions.type", "$.status.conditions.status"]
    - "$.status.conditions.lastTransitionTime": {"@now": null}
- "@groupBy": [["$.metadata.namespace", "$.metadata.name"], "$.status.conditions"]
```

This will unroll the `status.conditions` list, apply `@stamp` per each condition, and then
multiplexes conditions back into the list.

A port chosen once per object and kept for the object's lifetime:

```yaml
- "@stamp":
    - ["$.metadata.namespace", "$.metadata.name"]
    - "$.spec.nodePort": {"@rnd": [30000, 32767]}
```

Note: currently neither the key expression nor the value expression can to refer to the stamped
fields. 
