# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DBSP is a Go workspace implementing the DBSP circuit library for incremental computation. It translates DBSP theory (Z-sets, streams, operators, incrementalization) into a practical library with a JavaScript scripting frontend and Kubernetes operator integration.

## Workspace Structure

| Module path | Directory | Purpose |
|---|---|---|
| `github.com/l7mp/dbsp/engine` | `engine/` | Core DBSP circuit library |
| `github.com/l7mp/dbsp/js` | `js/` | JavaScript runtime CLI (`dbsp` binary) |
| `github.com/l7mp/dbsp/connectors/kubernetes` | `connectors/kubernetes/` | Kubernetes watch/patch connectors |
| `github.com/l7mp/dbsp/connectors/misc` | `connectors/misc/` | Pipe/channel connectors |
| `github.com/l7mp/dbsp/dcontroller` | `dcontroller/` | Kubernetes operator built on DBSP |

## Build and Test Commands

```bash
# Run all tests across all workspace modules
go test ./...

# Run tests for a specific module (from workspace root)
go test github.com/l7mp/dbsp/engine/...
go test github.com/l7mp/dbsp/js/...

# Run tests for a specific engine package
go test github.com/l7mp/dbsp/engine/zset
go test github.com/l7mp/dbsp/engine/operator
go test github.com/l7mp/dbsp/engine/circuit
go test github.com/l7mp/dbsp/engine/transform

# Run a specific test (Ginkgo matches on Describe/It names)
go test github.com/l7mp/dbsp/engine/operator -run "Select"

# Build the JavaScript runtime CLI
cd js && go build -o bin/dbsp .

# Run a DBSP script
./js/bin/dbsp examples/join_project.js
./js/bin/dbsp -v examples/join_project.js   # verbose / debug logs

# Format and organize imports
goimports -w .
```

## Engine Architecture (`engine/`)

### Package Structure

- **zset/**: Z-sets (weighted multisets) – the fundamental data structure. Elements implement `Key()` for identity.
- **expression/**: Expression interface with single `Evaluate(elem) (any, error)` method for predicates and projections.
- **operator/**: All DBSP operators classified by linearity:
  - Linear (`O^Δ = O`): Negate, Plus, Select, Project
  - Bilinear (three-term expansion): CartesianProduct
  - Non-linear (`D ∘ O ∘ ∫` wrapping): Distinct, Group, Unwind
- **circuit/**: Circuit model as directed graph. Node types: Input, Output, Operator, Delay (z⁻¹), Integrate (∫), Differentiate (D), Delta0 (δ₀).
- **transform/**: Implements Algorithm 6.4 – transforms circuit C into incremental version C^Δ.
- **executor/**: Runtime executor with state management for delays and integrators.
- **compiler/**: Compiles SQL and aggregate pipeline specs into circuits.
- **runtime/**: Shared pub/sub event bus (`Runtime`, `Publisher`, `Subscriber`) used by all modules.
- **datamodel/**: Structured data model (documents, relations, schema).

### Key Design Principles

1. **Circuits are first-class**: Everything is a circuit, including operators.
2. **No per-operator incrementalization**: Use Algorithm 6.4 (circuit transformation) instead.
3. **Mutable Z-sets**: `Insert` modifies in-place.
4. **Interface-based elements**: Users define their own element types implementing `Element.Key()`.
5. **Well-formedness**: Every cycle must contain at least one delay (z⁻¹). Use `circuit.Validate()` to check.

## JavaScript Runtime (`js/`)

The `js/` module provides an embedded JavaScript runtime (via [goja](https://github.com/dop251/goja)) so users can write DBSP scripts as `.js` files instead of a custom DSL. It replaced the former `shell/` module.

### Injected Globals

```javascript
// SQL compilation
sql.table(name, ddlCols)
sql.compile(query, { output }).incrementalize().validate()

// Aggregate compilation
aggregate.compile(pipeline, { inputs, output }).incrementalize().validate()

// Data flow
producer(topic[, entries])      // returns handle with .publish([[doc, weight], ...])
consumer(topic, callback)       // callback receives [[doc, weight], ...] per step

// Kubernetes connectors
producer.kubernetes.watch({ gvk, namespace, labels, topic })
consumer.kubernetes.patcher({ gvk, topic })
consumer.kubernetes.updater({ gvk, topic })
```

### Concurrency Model

goja is single-threaded; all JS execution runs on the main goroutine. The VM owns an event loop (`callbacks` channel, capacity 1024). Background goroutines (runtime producers/consumers, K8s watchers) queue closures via `vm.schedule(fn)`; the event loop drains them. The VM shuts down automatically when idle (~200 ms grace period with no pending callbacks).

### Key Types

- `VM` (`vm.go`): goja runtime + DBSP runtime + event loop + `schedule()`/`toJSEntries()`/`fromJSEntries()` helpers.
- `circuitHandle` (`circuit.go`): wraps `*circuit.Circuit`; provides `.incrementalize()` and `.validate()` JS methods.
- `producerHandle` (`producer.go`): wraps a publisher; provides `.publish()` JS method.

## Testing

Tests use Ginkgo v2 + Gomega (BDD style):

```go
var _ = Describe("Feature", func() {
    It("should do something", func() {
        Expect(result).To(Equal(expected))
    })
})
```

Key test patterns:
- **Engine**: verify normal execution equals incremental execution.
- **js/**: spawn `vm.RunFile()` in a goroutine, close VM after a grace period, assert no error.

## Code Style

- Comments follow Go style guide – complete sentences ending with periods.
- Mathematical notation in comments: σ (select), × (product), ∫ (integrate), D (differentiate), z⁻¹ (delay), δ₀ (delta-zero).
