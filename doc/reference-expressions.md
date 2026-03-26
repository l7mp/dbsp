# Reference: Expression Language

The expression language is the small functional language used inside DBSP pipelines. It appears in
aggregation pipelines, in Δ-controller controller specs, and anywhere a transformation needs to
compute a value from the current document.

Expressions are JSON or YAML values. A plain literal such as `42`, `"prod"`, or `true` evaluates to
itself. An operator expression is a JSON object with a single key that starts with `@`.

```yaml
"@eq": ["$.metadata.namespace", "prod"]
```

In practice, expressions are easiest to read if you imagine evaluating them against one current
document. In the examples below, assume the current document is roughly:

```yaml
metadata:
  name: web
  namespace: prod
  labels:
    app: web
    tier: frontend
spec:
  replicas: 3
  ports:
    - name: http
      port: 80
      protocol: TCP
    - name: metrics
      port: 9090
      protocol: TCP
status:
  readyReplicas: 2
```

## Context and shorthand

There are two evaluation contexts.

The document context is the main input object. The subject context is a temporary local value used
inside operators such as `@map`, `@filter`, and `@sortBy`.

- `$.field` means read a field from the current document.
- `$$.field` means read a field from the current subject.
- `"$."` means copy the whole current document.
- `"$$."` means return the current subject itself.

The `"$."`, `"$$."`, `$$.field`, and related subject-aware forms are part of the surface syntax of
the language. Internally they compile to operators such as `@copy`, `@subject`, `@getsub`, and
`@setsub`, but in user-facing expressions the shorthand forms are usually the clearest way to write
them.

This is the most important thing to keep in mind when reading pipelines.

```yaml
"@map":
  - "$$.name"
  - "$.spec.ports"
```

Here `$.spec.ports` reads the list from the document, but `$$.name` reads from each individual port
object while the map is iterating.

## Literals and constructors

Every JSON literal already works as an expression, but the explicit constructors are useful when you
want to make the type obvious or build nested structures.

Supported constructors are `@nil`, `@bool`, `@int`, `@float`, `@string`, `@list`, and `@dict`.

### `@nil`

Produces `null`.

```yaml
"@nil": null
```

Use this when you want an explicit null branch in `@cond` or `@switch`.

### `@bool`, `@int`, `@float`, `@string`

These convert or normalize a value to the given scalar type.

```yaml
"@string": "$.spec.replicas"
```

Intuitively, this is how you turn a numeric replica count into a string when constructing a
ConfigMap or an annotation.

```yaml
metadata:
  annotations:
    replicas: {"@string": "$.spec.replicas"}
```

Related examples:

```yaml
"@int": "7"
"@float": "$.status.utilization"
"@bool": "$.spec.enabled"
```

### `@list`

Builds a list by evaluating each element.

```yaml
"@list": ["$.metadata.name", "$.metadata.namespace", "frontend"]
```

This yields something like `["web", "prod", "frontend"]` and is useful when a later operator expects
a list input.

### `@dict`

Builds an object by evaluating each value expression.

```yaml
status:
  "@dict":
    name: "$.metadata.name"
    ready: "$.status.readyReplicas"
    desired: "$.spec.replicas"
```

This is the expression-level way to construct a new document shape. Note again that the literal
constructors `@bool`, `@int`, `@float`, `@string` are all optional, so the above is identical to:

```yaml
status:
  name: "$.metadata.name"
  ready: "$.status.readyReplicas"
  desired: "$.spec.replicas"
```

## Logical and conditional operators

These operators decide whether a record passes a filter, which branch to take, or which default
value to use.

### `@and` and `@or`

`@and` requires every condition to be true. `@or` requires at least one to be true. Both
short-circuit.

```yaml
"@and":
  - {"@eq": ["$.metadata.namespace", "prod"]}
  - {"@gt": ["$.spec.replicas", 1]}
```

This reads naturally as: keep only objects in `prod` that ask for more than one replica.

```yaml
"@or":
  - {"@eq": ["$.metadata.labels.tier", "frontend"]}
  - {"@eq": ["$.metadata.labels.tier", "edge"]}
```

This is a whitelist of acceptable tiers.

### `@not`

Negates a boolean condition.

```yaml
"@not": {"@eq": ["$.metadata.namespace", "kube-system"]}
```

This is a clean way to say “anything except system objects”.

### `@cond`

`@cond` is the expression-language `if/else`.

```yaml
"@cond":
  - {"@gte": ["$.status.readyReplicas", "$.spec.replicas"]}
  - "healthy"
  - "degraded"
```

This turns a readiness comparison into a readable status label.

### `@switch`

`@switch` is a multi-branch version of `@cond`. It evaluates case or result pairs in order and
returns the result of the first true case.

```yaml
"@switch":
  - - {"@eq": ["$.metadata.namespace", "prod"]}
    - "critical"
  - - {"@eq": ["$.metadata.namespace", "staging"]}
    - "important"
  - - true
    - "standard"
```

This is the natural choice when one field maps to several categories.

### `@definedOr`

Returns the first argument that evaluates to a non-null value.

```yaml
"@definedOr":
  - "$.metadata.annotations.owner"
  - "$.metadata.labels.app"
  - "unknown"
```

This is a fallback chain. Use the owner annotation if present, otherwise fall back to the app label,
otherwise use a constant default.

## Arithmetic operators

These operators work on numbers. Integer inputs stay integer when possible. Mixed integer and float
inputs promote to float.

### `@add` and `@sum`

Both add numbers, but they are usually used with different intent.

- `@add` is the general arithmetic operator.
- `@sum` is the aggregation-flavoured spelling when you sum numeric lists.

```yaml
"@add": ["$.status.readyReplicas", 1]
```

This means “pretend one more replica becomes ready”.

```yaml
"@sum": [80, 9090, 443]
```

This is just a total. In real pipelines, `@sum` often appears after values have already been
selected or extracted.

### `@sub`

Subtracts the second argument from the first.

```yaml
"@sub": ["$.spec.replicas", "$.status.readyReplicas"]
```

This computes how many replicas are still missing.

### `@mul`

Multiplies all arguments.

```yaml
"@mul": ["$.spec.replicas", 2]
```

Think of this as scaling a quantity.

### `@div`

Divides the first argument by the second.

```yaml
"@div": ["$.status.readyReplicas", "$.spec.replicas"]
```

This is the raw readiness ratio. With integer inputs, division is integer division, so if you need
fractions, coerce one side to float first.

```yaml
"@div": [{"@float": "$.status.readyReplicas"}, "$.spec.replicas"]
```

### `@mod`

Computes integer remainder.

```yaml
"@mod": ["$.spec.replicas", 2]
```

This is the standard parity check. The result is `0` for even and `1` for odd.

### `@neg`

Unary minus.

```yaml
"@neg": "$.status.readyReplicas"
```

This is mostly useful as part of a larger arithmetic expression.

## Comparison operators

These all return booleans. They work on numbers, and the ordering operators also support strings.

### `@eq` and `@neq`

Deep equality and inequality.

```yaml
"@eq": ["$.metadata.namespace", "prod"]
"@neq": ["$.metadata.labels.tier", "backend"]
```

The first checks exact namespace match. The second excludes one tier.

### `@gt`, `@gte`, `@lt`, `@lte`

Standard ordering comparisons.

```yaml
"@gte": ["$.status.readyReplicas", "$.spec.replicas"]
```

This is the common “is the rollout complete?” check.

```yaml
"@lt": ["$.metadata.name", "m"]
```

String ordering is lexicographic, which is mostly useful in sorting logic.

## Field and subject operators

These operators read and write values in the current document or the current subject.

### `@get` and `$.field`

Read a field from the current document.

```yaml
"$.metadata.name"
```

This is by far the most common form. The explicit version is:

```yaml
"@get": "metadata.name"
```

When the path itself needs quoting, use bracket notation.

```yaml
"$[\"metadata\"][\"labels\"][\"app\"]"
```

### `"$."` and internal `@copy`

Return the whole current document as a plain object.

```yaml
"$."
```

Use this when you want to pass the original object through or start from a full copy before adding
new fields. The explicit `@copy` form exists in the implementation, but the `"$."` shorthand is the
form you should normally write in user-facing pipelines.

### `@set`

Mutates the current document by setting a field, then returns the modified document.

```yaml
"@set": ["metadata.annotations.checked", true]
```

Intuitively, this is an in-place annotation write on the current record.

### `$$.field` and internal `@getsub`

Read from the current subject instead of the main document.

```yaml
"@map":
  - "$$.port"
  - "$.spec.ports"
```

The document gives the list of ports, and `$$.port` reads the `port` field from each individual port
object while iterating. Internally this is implemented by `@getsub`, but the shorthand is the
natural surface syntax.

### `"$$."` and internal `@subject`

Return the current subject itself.

```yaml
"@map":
  - "$$."
  - "$.spec.ports"
```

This is the identity mapping over the subject list. Internally this is represented as `@subject`,
but in user-facing expressions the `"$$."` shorthand is preferred.

### `@setsub`

Mutates the current subject and returns it.

```yaml
"@map":
  - {"@setsub": ["protocolSeen", true]}
  - "$.spec.ports"
```

This is useful when the subject is already the object you want to enrich inside a list traversal.
Unlike `@copy` and `@subject`, `@setsub` is not just an internal implementation detail: it is part
of the current expression language and may be written explicitly when needed.

### `@exists`

Checks whether a document field resolves successfully.

```yaml
"@exists": "metadata.labels.tier"
```

This is the cleanest presence test for optional fields.

## List operators

These operators reshape or summarize lists.

### `@map`

Apply an expression to every element of a list.

```yaml
"@map":
  - "$$.name"
  - "$.spec.ports"
```

This turns a list of port objects into a list of port names.

### `@filter`

Keep only the elements whose predicate is true.

```yaml
"@filter":
  - {"@eq": ["$$.protocol", "TCP"]}
  - "$.spec.ports"
```

This removes every non-TCP port from the list.

### `@sortBy`

Sort a list using a comparator expression. During comparison, the subject becomes an object with
two fields: `a` and `b`, the two candidate values being compared. The comparator must return `-1`,
`0`, or `1`.

```yaml
"@sortBy":
  - "@switch":
      - [{"@gt": ["$$.a", "$$.b"]}, 1]
      - [{"@eq": ["$$.a", "$$.b"]}, 0]
      - [true, -1]
  - [3, 1, 4, 1, 5]
```

This sorts the list in ascending order. The structure looks heavy, but the logic is simple: compare
two candidates and tell the sorter which one comes first.

### `@len`

Returns the length of a list, string, or map.

```yaml
"@len": "$.spec.ports"
```

This is the number of ports declared by the object.

### `@min` and `@max`

Return the smallest or largest numeric value.

```yaml
"@min": [80, 9090, 443]
"@max": [80, 9090, 443]
```

These are natural when a policy depends on the smallest limit or the largest observed value.

### `@lexmin` and `@lexmax`

Return the lexicographically smallest or largest value.

```yaml
"@lexmin": ["web", "api", "metrics"]
"@lexmax": ["web", "api", "metrics"]
```

This is string ordering rather than numeric ordering.

### `@in`

Membership test.

```yaml
"@in": ["frontend", ["frontend", "backend", "batch"]]
```

This is the natural way to express a whitelist.

### `@range`

Generates the integer list `[1..n]`.

```yaml
"@range": 4
```

This yields `[1, 2, 3, 4]`. It is handy when you need a fixed number of slots or iterations.

## String operators

These operators normalize, search, and reshape strings.

### `@regexp`

Regular-expression match.

```yaml
"@regexp": ["^web-", "$.metadata.name"]
```

This checks whether the name starts with `web-` using a regex rather than plain prefix matching.

### `@upper`, `@lower`, `@trim`

Standard normalization helpers.

```yaml
"@upper": "$.metadata.namespace"
"@lower": "PROD"
"@trim": "  web  "
```

These are typically used before comparison or when constructing normalized output strings.

### `@substring`

Extracts a portion of a string. Indexing is 1-based, SQL-style.

```yaml
"@substring": ["$.metadata.name", 1, 3]
```

If the name is `web`, this returns `web`. If the name were `frontend`, it would return `fro`.

### `@replace`

Replaces occurrences of one substring with another.

```yaml
"@replace": ["$.metadata.name", "web", "svc"]
```

This is useful when deriving related names with a predictable naming convention.

### `@split` and `@join`

Break a string into parts, or combine a list of strings into one string.

```yaml
"@split": ["prod,frontend,stable", ","]
```

This yields `[`prod`, `frontend`, `stable`]`.

```yaml
"@join": [["web", "prod", "blue"], "-"]
```

This yields `web-prod-blue`.

### `@startswith`, `@endswith`, `@contains`

Common substring tests.

```yaml
"@startswith": ["$.metadata.name", "web"]
"@endswith": ["$.metadata.name", "-canary"]
"@contains": ["$.metadata.name", "api"]
```

Use these when regex would be overkill.

### `@concat`

Concatenates strings after coercing arguments to strings.

```yaml
"@concat": ["$.metadata.name", "-", "$.metadata.namespace"]
```

This is the standard way to build annotation values, derived names, or log keys.

## Null, SQL boolean, and utility operators

These cover null checks, SQL-like truth handling, and a few helper functions.

### `@isnull` and `@isnil`

Both check whether a value is null.

```yaml
"@isnull": "$.metadata.annotations.owner"
"@isnil": "$.metadata.annotations.owner"
```

In the current implementation they are effectively the same kind of null test.

### `@sqlbool`

Normalizes SQL-style truth semantics by turning `null` into `false`.

```yaml
"@sqlbool": "$.spec.enabled"
```

This is useful when a nullable expression feeds a strict boolean operator.

### `@noop`

Returns `null` and does nothing.

```yaml
"@noop": null
```

This is a placeholder branch result when you want “no value” explicitly.

### `@hash`

Computes a deterministic short hash of a value.

```yaml
"@concat": ["cfg-", "$.metadata.name", "-", {"@hash": "$.spec"}]
```

This is a classic pattern for stable derived names that should change when a spec changes.

### `@rnd`

Returns a random number between a minimum and a maximum. For integer inputs, both ends are
inclusive.

```yaml
"@rnd": [1, 3]
```

This is mainly useful in examples, synthetic data, or quick experiments rather than deterministic
production logic.

### `@abs`

Absolute value.

```yaml
"@abs": {"@sub": ["$.spec.replicas", "$.status.readyReplicas"]}
```

This turns a signed gap into an unsigned magnitude.

### `@floor` and `@ceil`

Round down or round up to an integer.

```yaml
"@floor": 2.9
"@ceil": 2.1
```

These are useful when converting floating-point ratios into bucket or threshold values.

### `@now`

Returns the current UTC timestamp in RFC3339 format.

```yaml
"@now": null
```

The common use is to stamp generated output with a reconciliation time.

```yaml
metadata:
  annotations:
    reconciled-at: {"@now": null}
```

## Putting it together

Real expressions are usually small compositions of the operators above. For example, the following
builds a compact health summary:

```yaml
"@dict":
  name: "$.metadata.name"
  namespace: "$.metadata.namespace"
  healthy: {"@gte": ["$.status.readyReplicas", "$.spec.replicas"]}
  missing: {"@sub": ["$.spec.replicas", "$.status.readyReplicas"]}
  labelKey: {"@concat": ["$.metadata.namespace", "/", "$.metadata.name"]}
```

This is typical DBSP expression style: read a few fields, compute a few derived values, and produce
exactly the object shape that the next pipeline stage needs.
