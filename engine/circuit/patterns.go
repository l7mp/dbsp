package circuit

import (
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/operator"
)

// Join builds: σ_predicate(A × B).
// This creates a circuit that computes the join of two inputs.
func Join(name string, predicate expression.Expression) *Circuit {
	c := New(name)

	c.AddNode(Input("left"))
	c.AddNode(Input("right"))
	c.AddNode(Op("product", operator.NewCartesianProduct()))
	c.AddNode(Op("select", operator.NewSelect(predicate)))
	c.AddNode(Output("out"))

	c.AddEdge(NewEdge("left", "product", 0))
	c.AddEdge(NewEdge("right", "product", 1))
	c.AddEdge(NewEdge("product", "select", 0))
	c.AddEdge(NewEdge("select", "out", 0))

	return c
}

// BilinearIncremental creates the incremental pattern for a bilinear operator.
// Implements: Δ(∫a ⊗ ∫b) = (Δa ⊗ ∫b) + (∫a ⊗ Δb) + (Δa ⊗ Δb).
func BilinearIncremental(name string, bilinearOp operator.Operator) *Circuit {
	c := New(name)

	// Input deltas.
	c.AddNode(Input("delta_a"))
	c.AddNode(Input("delta_b"))

	// Integrators for accumulated state.
	c.AddNode(Integrate("int_a"))
	c.AddNode(Integrate("int_b"))
	c.AddEdge(NewEdge("delta_a", "int_a", 0))
	c.AddEdge(NewEdge("delta_b", "int_b", 0))

	// Term 1: Δa ⊗ ∫b.
	c.AddNode(Op("term1", bilinearOp))
	c.AddEdge(NewEdge("delta_a", "term1", 0))
	c.AddEdge(NewEdge("int_b", "term1", 1))

	// Term 2: ∫a ⊗ Δb.
	c.AddNode(Op("term2", bilinearOp))
	c.AddEdge(NewEdge("int_a", "term2", 0))
	c.AddEdge(NewEdge("delta_b", "term2", 1))

	// Term 3: Δa ⊗ Δb.
	c.AddNode(Op("term3", bilinearOp))
	c.AddEdge(NewEdge("delta_a", "term3", 0))
	c.AddEdge(NewEdge("delta_b", "term3", 1))

	// Sum term1 + term2.
	c.AddNode(Op("sum12", operator.NewPlus()))
	c.AddEdge(NewEdge("term1", "sum12", 0))
	c.AddEdge(NewEdge("term2", "sum12", 1))

	// Sum (term1 + term2) + term3.
	c.AddNode(Op("sum_all", operator.NewPlus()))
	c.AddEdge(NewEdge("sum12", "sum_all", 0))
	c.AddEdge(NewEdge("term3", "sum_all", 1))

	// Output.
	c.AddNode(Output("out"))
	c.AddEdge(NewEdge("sum_all", "out", 0))

	return c
}

// DistinctKeyedIncremental creates the incremental circuit for distinct_π.
// It uses the generic aggregate reducer equivalent for distinct_π.
//
//	delta ──→ aggregate_keyed(distinct_π) ──→ out
func DistinctKeyedIncremental(name string) *Circuit {
	c := New(name)

	c.AddNode(Input("delta"))
	c.AddNode(Op("A", operator.NewDistinctPi()))
	c.AddNode(Output("out"))

	c.AddEdge(NewEdge("delta", "A", 0))
	c.AddEdge(NewEdge("A", "out", 0))

	return c
}

// NonLinearIncremental creates the incremental pattern for a non-linear operator.
// Implements: D ∘ O ∘ ∫.
func NonLinearIncremental(name string, op operator.Operator) *Circuit {
	c := New(name)

	// Input delta.
	c.AddNode(Input("delta"))

	// Integrate to get full state.
	c.AddNode(Integrate("int"))
	c.AddEdge(NewEdge("delta", "int", 0))

	// Apply non-linear operator.
	c.AddNode(Op("op", op))
	c.AddEdge(NewEdge("int", "op", 0))

	// Differentiate to get output delta.
	c.AddNode(Differentiate("diff"))
	c.AddEdge(NewEdge("op", "diff", 0))

	// Output.
	c.AddNode(Output("out"))
	c.AddEdge(NewEdge("diff", "out", 0))

	return c
}
