package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/expression"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/operator"
)

// NOTE: Regularizer intentionally does not add a trailing Distinct after
// project(lexmin(values)). The GroupBy(identity, subject) + lexmin projection
// is already set-producing by construction: representatives are keyed by
// document identity (content hash), deterministic, and unique per group, so
// downstream H never needs an extra clamp stage for this chain.

type regularizer struct{}

// NewRegularizer creates a regularizer transform.
//
// For each output node, the transform inserts:
//
//	pred_0..pred_n -> sum -> group_by(identity, subject) -> project(lexmin(values)) -> output
//
// Semantics:
//   - sum normalizes multi-predecessor outputs to a single stream.
//   - group_by(identity, subject) collects rows per document identity
//     (content hash).
//   - project(lexmin(values)) selects one deterministic representative document
//     (arg-lexmin over full documents) for each identity.
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

	// 2. Add Sum + GroupBy(identity) + Project(lexmin(values)).
	coeffs := make([]int, len(inEdges))
	for i := range coeffs {
		coeffs[i] = 1
	}

	sumID := "_sum_" + output.ID
	if err := c.AddNode(circuit.Op(sumID, operator.NewLinearCombination(coeffs))); err != nil {
		return fmt.Errorf("regularizer: add sum node: %w", err)
	}

	grpID := "_grp_" + output.ID
	// Group by document identity: the key is the document's content hash,
	// computed directly (one canonical serialization per element, the same
	// cost the Z-set machinery already pays per insertion). The marshalable
	// fallback expression preserves the grouping semantics for circuits
	// reconstructed from JSON.
	hashKey, err := dbspexpr.Compile([]byte(`{"@hash": "$."}`))
	if err != nil {
		return fmt.Errorf("regularizer: compile hash key: %w", err)
	}
	byIdentity := expression.NewCompiled(func(ctx *expression.EvalContext) (any, error) {
		doc := ctx.Document()
		if doc == nil {
			return nil, fmt.Errorf("regularizer group key: missing document")
		}
		return doc.Hash(), nil
	}, hashKey)
	if err := c.AddNode(circuit.Op(grpID, operator.NewGroupBy(byIdentity, dbspexpr.NewSubject()))); err != nil {
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
