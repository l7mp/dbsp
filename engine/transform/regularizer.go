package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/operator"
)

// NOTE: Regularizer intentionally does not add a trailing Distinct after
// project(lexmin(values)). The GroupBy(pk, subject) + lexmin projection is
// already set-producing by construction: representatives are PK-keyed,
// deterministic, and unique per group, so downstream H never needs an extra
// clamp stage for this chain.

type regularizer struct{}

// NewRegularizer creates a regularizer transform.
//
// For each output node, the transform inserts:
//
//	pred_0..pred_n -> sum -> group_by(pk, subject) -> project(lexmin(values)) -> output
//
// Semantics:
//   - sum normalizes multi-predecessor outputs to a single stream.
//   - group_by(pk, subject) collects rows per primary key.
//   - project(lexmin(values)) selects one deterministic representative document
//     (arg-lexmin over full documents) for each primary key.
func NewRegularizer() Transformer {
	return &regularizer{}
}

func (t *regularizer) Name() TransformerType { return Regularizer }

func (t *regularizer) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	clone := c.Clone()

	for _, n := range c.Outputs() {
		if err := injectRegularizer(clone, n); err != nil {
			return nil, err
		}
	}

	return clone, nil
}

func injectRegularizer(c *circuit.Circuit, output *circuit.Node) error {
	inEdges := c.EdgesTo(output.ID)

	// 1. Open up output node.
	for _, e := range inEdges {
		if err := c.RemoveEdge(e.From, e.To, e.Port); err != nil {
			return fmt.Errorf("regularizer: remove pred→output edge: %w", err)
		}
	}

	// 2. Add Sum + GroupBy(pk) + Project(lexmin(values)).
	coeffs := make([]int, len(inEdges))
	for i := range coeffs {
		coeffs[i] = 1
	}

	sumID := "_sum_" + output.ID
	if err := c.AddNode(circuit.Op(sumID, operator.NewLinearCombination(coeffs))); err != nil {
		return fmt.Errorf("regularizer: add sum node: %w", err)
	}

	grpID := "_grp_" + output.ID
	if err := c.AddNode(circuit.Op(grpID, operator.NewGroupBy(nil, dbspexpr.NewSubject()))); err != nil {
		return fmt.Errorf("regularizer: add group_by node: %w", err)
	}

	regID := "_reg_" + output.ID
	if err := c.AddNode(circuit.Op(regID, operator.NewProject(dbspexpr.NewLexMin(dbspexpr.NewGet("values"))))); err != nil {
		return fmt.Errorf("regularizer: add lexmin project node: %w", err)
	}

	// 3. Close down the circuit.
	for i, e := range inEdges {
		if err := c.AddEdge(circuit.NewEdge(e.From, sumID, i)); err != nil {
			return fmt.Errorf("regularizer: wire pred→sum: %w", err)
		}
	}

	if err := c.AddEdge(circuit.NewEdge(sumID, grpID, 0)); err != nil {
		return fmt.Errorf("regularizer: wire sum→group_by: %w", err)
	}

	if err := c.AddEdge(circuit.NewEdge(grpID, regID, 0)); err != nil {
		return fmt.Errorf("regularizer: wire group_by→lexmin: %w", err)
	}

	if err := c.AddEdge(circuit.NewEdge(regID, output.ID, 0)); err != nil {
		return fmt.Errorf("regularizer: wire lexmin→output: %w", err)
	}

	return nil
}
