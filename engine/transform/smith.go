package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

// smithPredictor is the Smith dead-time compensator in its known-window
// form: the desired-state reconciler with the feedback compared against a
// prediction instead of the raw observation. Per reconciled pair the
// transform injects
//
//	U  = ∫(δD − δS)                    (the pending correction, emitted)
//	δS = dist^Δ(δY + z⁻¹U − z⁻ᴷU)      (the Smith prediction delta)
//
// where the two taps come from a K-cell delay line on the emission: the
// window of in-flight commands, telescoped (the previous emission enters
// the window, the K-steps-old one leaves). K is the assumed feedback dead
// time, in circuit steps, and K = 1 degenerates to the plain Reconciler.
type smithPredictor struct {
	k     int
	pairs []ReconcilerPair
}

// NewSmithPredictor creates a Smith dead-time compensator transform with
// compensation window k for the given input/output pairs. With no pairs,
// self-referential pairs are auto-detected exactly as for the Reconciler.
func NewSmithPredictor(k int, pairs ...ReconcilerPair) Transformer {
	return &smithPredictor{k: k, pairs: pairs}
}

func (t *smithPredictor) Name() TransformerType { return SmithPredictor }

func (t *smithPredictor) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	if t.k < 2 {
		return nil, fmt.Errorf("smith: compensation window k is %d, need at least 2 (k = 1 is the Reconciler)", t.k)
	}

	clone := c.Clone()
	pairs := t.pairs
	if len(pairs) == 0 {
		pairs = detectSelfReferentialPairs(clone)
	}
	if len(pairs) == 0 {
		// No-op when no pairs are available, mirroring the Reconciler.
		return clone, nil
	}

	seenInputs := map[string]bool{}
	seenOutputs := map[string]bool{}
	for _, p := range pairs {
		if seenInputs[p.InputID] {
			return nil, fmt.Errorf("smith: input %q used in multiple pairs", p.InputID)
		}
		if seenOutputs[p.OutputID] {
			return nil, fmt.Errorf("smith: output %q used in multiple pairs", p.OutputID)
		}
		seenInputs[p.InputID] = true
		seenOutputs[p.OutputID] = true
	}

	for _, p := range pairs {
		if err := injectSmithLoop(clone, p, t.k); err != nil {
			return nil, err
		}
	}

	return clone, nil
}

func injectSmithLoop(c *circuit.Circuit, pair ReconcilerPair, k int) error {
	inputNode := c.Node(pair.InputID)
	outputNode := c.Node(pair.OutputID)
	if inputNode == nil {
		return fmt.Errorf("smith: input node %q not found", pair.InputID)
	}
	if outputNode == nil {
		return fmt.Errorf("smith: output node %q not found", pair.OutputID)
	}
	if inputNode.Kind() != operator.KindInput {
		return fmt.Errorf("smith: node %q is %s, not input", pair.InputID, inputNode.Kind())
	}
	if outputNode.Kind() != operator.KindOutput {
		return fmt.Errorf("smith: node %q is %s, not output", pair.OutputID, outputNode.Kind())
	}
	if c.Node("_rec_"+pair.OutputID+"_acc") != nil {
		return fmt.Errorf("smith: output %q already carries a Reconciler loop; apply SmithPredictor instead of (not on top of) Reconciler", pair.OutputID)
	}

	prefix := "_smith_" + pair.OutputID
	accID := prefix + "_acc"
	if c.Node(accID) != nil {
		return fmt.Errorf("smith: output %q already carries a Smith loop", pair.OutputID)
	}

	inEdges := c.EdgesTo(pair.OutputID)
	if len(inEdges) == 0 {
		return fmt.Errorf("smith: output node %q has no incoming edges", pair.OutputID)
	}

	sumID := prefix + "_sum"
	subID := prefix + "_sub"
	delayID := prefix + "_delay"
	winID := prefix + "_win"
	distID := prefix + "_dist"

	// Fold multiple predecessors into one desired-delta stream, as the
	// Reconciler does.
	predID := ""
	if len(inEdges) == 1 {
		predID = inEdges[0].From
	} else {
		maxPort := 0
		for _, e := range inEdges {
			if e.Port > maxPort {
				maxPort = e.Port
			}
		}
		coeffs := make([]int, maxPort+1)
		for i := range coeffs {
			coeffs[i] = 1
		}
		if err := c.AddNode(circuit.Op(sumID, operator.NewLinearCombination(coeffs))); err != nil {
			return fmt.Errorf("smith: add sum node: %w", err)
		}
		for _, e := range inEdges {
			if err := c.AddEdge(circuit.NewEdge(e.From, sumID, e.Port)); err != nil {
				return fmt.Errorf("smith: wire pred to sum: %w", err)
			}
		}
		predID = sumID
	}

	// U = ∫(δD − δS): the Reconciler's sub/acc/delay feedback, with the
	// prediction delta in place of the raw feedback.
	if err := c.AddNode(circuit.Op(subID, operator.NewMinus())); err != nil {
		return fmt.Errorf("smith: add sub node: %w", err)
	}
	if err := c.AddNode(circuit.Op(accID, operator.NewPlus())); err != nil {
		return fmt.Errorf("smith: add acc node: %w", err)
	}
	if err := c.AddNode(circuit.Delay(delayID)); err != nil {
		return fmt.Errorf("smith: add delay node: %w", err)
	}

	// The window delay line: the acc feedback delay doubles as the entry
	// tap z⁻¹U; chaining k−1 more delays yields the exit tap z⁻ᴷU.
	chain := make([]string, 0, k-1)
	for i := 2; i <= k; i++ {
		wID := fmt.Sprintf("%s_w%d", prefix, i)
		if err := c.AddNode(circuit.Delay(wID)); err != nil {
			return fmt.Errorf("smith: add window delay %d: %w", i, err)
		}
		chain = append(chain, wID)
	}

	// δS = dist^Δ(δY + z⁻¹U − z⁻ᴷU): the incrementalizer compiles the
	// distinct; its integral is the Smith prediction.
	if err := c.AddNode(circuit.Op(winID, operator.NewLinearCombination([]int{1, 1, -1}))); err != nil {
		return fmt.Errorf("smith: add window sum node: %w", err)
	}
	if err := c.AddNode(circuit.Op(distID, operator.NewDistinct())); err != nil {
		return fmt.Errorf("smith: add prediction distinct node: %w", err)
	}

	for _, e := range inEdges {
		if err := c.RemoveEdge(e.From, pair.OutputID, e.Port); err != nil {
			return fmt.Errorf("smith: remove pred to output edge: %w", err)
		}
	}

	wire := func(from, to string, port int) error {
		if err := c.AddEdge(circuit.NewEdge(from, to, port)); err != nil {
			return fmt.Errorf("smith: wire %s to %s: %w", from, to, err)
		}
		return nil
	}
	if err := wire(predID, subID, 0); err != nil {
		return err
	}
	if err := wire(distID, subID, 1); err != nil {
		return err
	}
	if err := wire(subID, accID, 0); err != nil {
		return err
	}
	if err := wire(delayID, accID, 1); err != nil {
		return err
	}
	if err := wire(accID, delayID, 0); err != nil {
		return err
	}
	prev := delayID
	for _, wID := range chain {
		if err := wire(prev, wID, 0); err != nil {
			return err
		}
		prev = wID
	}
	if err := wire(pair.InputID, winID, 0); err != nil {
		return err
	}
	if err := wire(delayID, winID, 1); err != nil {
		return err
	}
	if err := wire(prev, winID, 2); err != nil {
		return err
	}
	if err := wire(winID, distID, 0); err != nil {
		return err
	}
	if err := wire(accID, pair.OutputID, 0); err != nil {
		return err
	}

	return nil
}
