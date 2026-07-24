package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

type distincter struct{}

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

func (t *distincter) Name() TransformerType { return Distincter }

func (t *distincter) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	clone := c.Clone()

	for _, n := range c.Outputs() {
		if err := injectDistincter(clone, n); err != nil {
			return nil, err
		}
	}

	return clone, nil
}

func injectDistincter(c *circuit.Circuit, output *circuit.Node) error {
	inEdges := c.EdgesTo(output.ID)

	for _, e := range inEdges {
		if err := c.RemoveEdge(e.From, e.To, e.Port); err != nil {
			return fmt.Errorf("distincter: remove pred to output edge: %w", err)
		}
	}

	// The distinct has arity 1, so multi-predecessor outputs are folded
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
