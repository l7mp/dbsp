package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
)

type inputIntegrators struct {
	inputs map[string]bool
}

// NewInputIntegrators creates a transform that inserts an Integrate operator
// directly after each input node.
//
// This adapts delta-style sources (for example watch streams) to snapshot
// query semantics by reconstructing each input's running state.
func NewInputIntegrators(inputs ...string) Transformer {
	if len(inputs) == 0 {
		return &inputIntegrators{}
	}
	selected := make(map[string]bool, len(inputs))
	for _, name := range inputs {
		selected[name] = true
	}
	return &inputIntegrators{inputs: selected}
}

func (t *inputIntegrators) Name() TransformerType { return InputIntegrators }

func (t *inputIntegrators) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	if c == nil {
		return nil, fmt.Errorf("input integrators: nil circuit")
	}

	clone := c.Clone()

	if len(t.inputs) > 0 {
		inputSet := make(map[string]bool, len(c.Inputs()))
		for _, in := range c.Inputs() {
			inputSet[in.ID] = true
		}
		for name := range t.inputs {
			if !inputSet[name] {
				return nil, fmt.Errorf("input integrators: input %q not found", name)
			}
		}
	}

	for _, in := range c.Inputs() {
		if len(t.inputs) > 0 && !t.inputs[in.ID] {
			continue
		}

		integratorID := in.ID + "_int"
		if clone.Node(integratorID) != nil {
			return nil, fmt.Errorf("input integrators: node %q already exists", integratorID)
		}

		if err := clone.AddNode(circuit.Integrate(integratorID)); err != nil {
			return nil, fmt.Errorf("input integrators: add node %q: %w", integratorID, err)
		}

		outEdges := append([]*circuit.Edge(nil), clone.EdgesFrom(in.ID)...)
		for _, e := range outEdges {
			if err := clone.RemoveEdge(e.From, e.To, e.Port); err != nil {
				return nil, fmt.Errorf("input integrators: remove edge %q -> %q (port %d): %w", e.From, e.To, e.Port, err)
			}
			if err := clone.AddEdge(circuit.NewEdge(integratorID, e.To, e.Port)); err != nil {
				return nil, fmt.Errorf("input integrators: add edge %q -> %q (port %d): %w", integratorID, e.To, e.Port, err)
			}
		}

		if err := clone.AddEdge(circuit.NewEdge(in.ID, integratorID, 0)); err != nil {
			return nil, fmt.Errorf("input integrators: add edge %q -> %q: %w", in.ID, integratorID, err)
		}
	}

	return clone, nil
}
