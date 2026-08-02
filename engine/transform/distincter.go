package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/expression"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/operator"
)

type distincter struct {
	key expression.Expression
}

// NewDistincter creates a distincter transform: for each output node it
// inserts
//
//	pred -> distinct -> output          (single predecessor)
//	pred_0..pred_n -> sum -> distinct -> output
//
// making every output set-valued (each document carried with weight at most
// one). A closed-loop output must be set-typed for the loop to quiesce: the
// observed feedback is a set, so a multi-derived desired document (weight 2
// after a join, say) would leave a permanent residual in the loop
// comparison U = ∫(δD − δY_U) and the loop would re-emit forever. The
// plant's own set semantics absorbs duplicate actuations but cannot repair
// the controller's comparison.
func NewDistincter() Transformer {
	return &distincter{}
}

// NewDistincterKeyed creates a keyed distincter (distinct_π): instead of a
// plain distinct it inserts
//
//	pred(s) [-> sum] -> group_by(π) -> project(lexmin(values)) -> output
//
// selecting one deterministic representative document per key. The output
// is a key-functional set: per key at most one document, carried with
// weight one, and when the current representative is retracted the
// incremental form retracts it and asserts the next one. This is the
// contract a plant-writing connector folds on (each document names a
// distinct target object), restored for pipelines with legitimate
// multiplicity; key-functional pipelines need no clamp at all.
func NewDistincterKeyed(key expression.Expression) Transformer {
	return &distincter{key: key}
}

func (t *distincter) Name() TransformerType { return Distincter }

func (t *distincter) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	clone := c.Clone()

	for _, n := range c.Outputs() {
		if err := t.inject(clone, n); err != nil {
			return nil, err
		}
	}

	return clone, nil
}

func (t *distincter) inject(c *circuit.Circuit, output *circuit.Node) error {
	inEdges := c.EdgesTo(output.ID)

	for _, e := range inEdges {
		if err := c.RemoveEdge(e.From, e.To, e.Port); err != nil {
			return fmt.Errorf("distincter: remove pred to output edge: %w", err)
		}
	}

	// The clamp has arity 1, so multi-predecessor outputs are folded
	// into one stream first; a single predecessor wires in directly.
	predID := ""
	if len(inEdges) == 1 {
		predID = inEdges[0].From
	} else {
		coeffs := make([]int, len(inEdges))
		for i := range coeffs {
			coeffs[i] = 1
		}
		sumID := "_sum_" + output.ID
		if err := c.AddNode(circuit.Op(sumID, operator.NewLinearCombination(coeffs))); err != nil {
			return fmt.Errorf("distincter: add sum node: %w", err)
		}
		for i, e := range inEdges {
			if err := c.AddEdge(circuit.NewEdge(e.From, sumID, i)); err != nil {
				return fmt.Errorf("distincter: wire pred to sum: %w", err)
			}
		}
		predID = sumID
	}

	if t.key == nil {
		dstID := "_dst_" + output.ID
		if err := c.AddNode(circuit.Op(dstID, operator.NewDistinct())); err != nil {
			return fmt.Errorf("distincter: add distinct node: %w", err)
		}
		if err := c.AddEdge(circuit.NewEdge(predID, dstID, 0)); err != nil {
			return fmt.Errorf("distincter: wire pred to distinct: %w", err)
		}
		if err := c.AddEdge(circuit.NewEdge(dstID, output.ID, 0)); err != nil {
			return fmt.Errorf("distincter: wire distinct to output: %w", err)
		}
		return nil
	}

	// Keyed form: group_by(π, subject) collects the candidate documents per
	// key; project(lexmin(values)) selects the representative. The pair is
	// set-producing by construction (one deterministic document per group),
	// so no trailing distinct is needed.
	grpID := "_grp_" + output.ID
	if err := c.AddNode(circuit.Op(grpID, operator.NewGroupBy(t.key, dbspexpr.NewSubject()))); err != nil {
		return fmt.Errorf("distincter: add group_by node: %w", err)
	}
	regID := "_rep_" + output.ID
	if err := c.AddNode(circuit.Op(regID, operator.NewProject(dbspexpr.NewLexMin(dbspexpr.NewGetField("values"))))); err != nil {
		return fmt.Errorf("distincter: add lexmin project node: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(predID, grpID, 0)); err != nil {
		return fmt.Errorf("distincter: wire pred to group_by: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(grpID, regID, 0)); err != nil {
		return fmt.Errorf("distincter: wire group_by to lexmin: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(regID, output.ID, 0)); err != nil {
		return fmt.Errorf("distincter: wire lexmin to output: %w", err)
	}

	return nil
}
