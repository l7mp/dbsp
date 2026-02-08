# DBSP - Incremental Computation in Go

A Go implementation of [DBSP (Database Stream Processing)](https://arxiv.org/abs/2203.16684), a mathematical framework for incremental view maintenance and streaming computation.

## Overview

DBSP represents computations as circuits operating on Z-sets (weighted multisets). Given a circuit C, DBSP can automatically derive an incremental version C^Δ that processes only changes, dramatically improving performance for large datasets with small updates.

## Installation

```bash
go get github.com/l7mp/dbsp
```

## Quick Example

```go
// Build a circuit: input -> filter(x > 5) -> output
c := circuit.New("example")
c.AddNode(circuit.Input("in"))
c.AddNode(circuit.Op("filter", operator.NewSelect("σ",
    expr.Func(func(e zset.Element) (any, error) {
        return e.(Record).Value > 5, nil
    }))))
c.AddNode(circuit.Output("out"))
c.AddEdge(circuit.NewEdge("in", "filter", 0))
c.AddEdge(circuit.NewEdge("filter", "out", 0))

// Incrementalize and execute
incr, _ := transform.Incrementalize(c)
exec, _ := execute.NewExecutor(incr)

// Process deltas instead of full state
delta := zset.New()
delta.Insert(Record{ID: "a", Value: 10}, 1)  // insert
delta.Insert(Record{ID: "b", Value: 3}, -1)  // delete
output, _ := exec.Execute(map[string]zset.ZSet{"in": delta})
```

## Packages

| Package | Description |
|---------|-------------|
| `zset` | Z-sets (weighted multisets) - the core data structure |
| `expr` | Expression interface for predicates and projections |
| `operator` | Operators: Select, Project, Plus, CartesianProduct, Distinct, Group |
| `circuit` | Circuit model with nodes (operators, delays, integrators) and edges |
| `transform` | Incrementalization (Algorithm 6.4 from DBSP paper) |
| `execute` | Circuit execution with state management |
| `relational` | SQL-like Row type and helpers for relational algebra |

## Key Concepts

- **Z-sets**: Maps from elements to integer weights. Positive = present, negative = deleted.
- **Operators**: Classified by linearity (Linear, Bilinear, NonLinear) for incrementalization.
- **Circuits**: Directed graphs with feedback loops via delay (z⁻¹) nodes.
- **Incrementalization**: Transforms circuit C to C^Δ where `D(C(∫Δs)) = C^Δ(Δs)`.

## License

MIT License - see [LICENSE](LICENSE) file.
