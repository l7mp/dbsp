package transform

import (
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

// ReconcilerPair identifies a self-referential input-output pair.
type ReconcilerPair struct {
	InputID  string
	OutputID string
}

type reconciler struct {
	pairs []ReconcilerPair
}

func NewReconciler(pairs ...ReconcilerPair) Transformer {
	return &reconciler{pairs: pairs}
}

func (t *reconciler) Name() TransformerType { return Reconciler }

func (t *reconciler) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	clone := c.Clone()
	pairs := t.pairs
	if len(pairs) == 0 {
		pairs = detectSelfReferentialPairs(clone)
	}
	if len(pairs) == 0 {
		// No-op when no pairs are available. This lets callers treat Reconciler as
		// an optional transform in mixed pipelines.
		return clone, nil
	}

	seenInputs := map[string]bool{}
	seenOutputs := map[string]bool{}
	for _, p := range pairs {
		if seenInputs[p.InputID] {
			return nil, fmt.Errorf("reconciler: input %q used in multiple pairs", p.InputID)
		}
		if seenOutputs[p.OutputID] {
			return nil, fmt.Errorf("reconciler: output %q used in multiple pairs", p.OutputID)
		}
		seenInputs[p.InputID] = true
		seenOutputs[p.OutputID] = true
	}

	for _, p := range pairs {
		if err := injectReconcilerLoop(clone, p); err != nil {
			return nil, err
		}
	}

	return clone, nil
}

func detectSelfReferentialPairs(c *circuit.Circuit) []ReconcilerPair {
	inputStems := map[string]string{}
	for _, n := range c.Inputs() {
		stem := strings.TrimPrefix(n.ID, "input_")
		if stem != n.ID {
			inputStems[stem] = n.ID
		}
	}

	pairs := []ReconcilerPair{}
	for _, n := range c.Outputs() {
		stem := strings.TrimPrefix(n.ID, "output_")
		if stem != n.ID {
			if inputID, ok := inputStems[stem]; ok {
				pairs = append(pairs, ReconcilerPair{InputID: inputID, OutputID: n.ID})
			}
		}
	}

	return pairs
}

func injectReconcilerLoop(c *circuit.Circuit, pair ReconcilerPair) error {
	inputNode := c.Node(pair.InputID)
	outputNode := c.Node(pair.OutputID)
	if inputNode == nil {
		return fmt.Errorf("reconciler: input node %q not found", pair.InputID)
	}
	if outputNode == nil {
		return fmt.Errorf("reconciler: output node %q not found", pair.OutputID)
	}
	if inputNode.Kind() != operator.KindInput {
		return fmt.Errorf("reconciler: node %q is %s, not input", pair.InputID, inputNode.Kind())
	}
	if outputNode.Kind() != operator.KindOutput {
		return fmt.Errorf("reconciler: node %q is %s, not output", pair.OutputID, outputNode.Kind())
	}

	inEdges := c.EdgesTo(pair.OutputID)
	if len(inEdges) != 1 {
		return fmt.Errorf("reconciler: output node %q has %d incoming edges, expected 1", pair.OutputID, len(inEdges))
	}
	predEdge := inEdges[0]
	predID := predEdge.From

	prefix := "_rec_" + pair.OutputID
	subID := prefix + "_sub"
	accID := prefix + "_acc"
	delayID := prefix + "_delay"

	if err := c.AddNode(circuit.Op(subID, operator.NewMinus())); err != nil {
		return fmt.Errorf("reconciler: add sub node: %w", err)
	}
	if err := c.AddNode(circuit.Op(accID, operator.NewPlus())); err != nil {
		return fmt.Errorf("reconciler: add acc node: %w", err)
	}
	if err := c.AddNode(circuit.Delay(delayID)); err != nil {
		return fmt.Errorf("reconciler: add delay node: %w", err)
	}

	if err := c.RemoveEdge(predID, pair.OutputID, predEdge.Port); err != nil {
		return fmt.Errorf("reconciler: remove pred→output edge: %w", err)
	}

	if err := c.AddEdge(circuit.NewEdge(predID, subID, 0)); err != nil {
		return fmt.Errorf("reconciler: wire pred→sub: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(pair.InputID, subID, 1)); err != nil {
		return fmt.Errorf("reconciler: wire input→sub: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(subID, accID, 0)); err != nil {
		return fmt.Errorf("reconciler: wire sub→acc: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(delayID, accID, 1)); err != nil {
		return fmt.Errorf("reconciler: wire delay→acc: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(accID, delayID, 0)); err != nil {
		return fmt.Errorf("reconciler: wire acc→delay: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(accID, pair.OutputID, 0)); err != nil {
		return fmt.Errorf("reconciler: wire acc→output: %w", err)
	}

	return nil
}
