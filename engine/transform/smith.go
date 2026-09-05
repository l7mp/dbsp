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
// where the two taps delimit the window of in-flight commands, telescoped
// (the previous emission enters the window, the K-steps-old one leaves):
// the acc feedback delay provides the entry tap z⁻¹U, and a single
// z⁻⁽ᴷ⁻¹⁾ ring delay on it provides the exit tap z⁻ᴷU, so the window
// costs O(1) nodes and O(1) work per step at any K. K is the assumed
// feedback dead time, in circuit steps, and K = 1 degenerates to the
// plain Reconciler.
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
	wexitID := prefix + "_wexit"
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
	if err := c.AddNode(circuit.Delay(delayID, 1)); err != nil {
		return fmt.Errorf("smith: add delay node: %w", err)
	}

	// The window taps: the acc feedback delay doubles as the entry tap
	// z⁻¹U; a single z⁻⁽ᵏ⁻¹⁾ ring delay on it yields the exit tap z⁻ᴷU.
	if err := c.AddNode(circuit.Delay(wexitID, k-1)); err != nil {
		return fmt.Errorf("smith: add window exit delay: %w", err)
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
	if err := wire(delayID, wexitID, 0); err != nil {
		return err
	}
	if err := wire(pair.InputID, winID, 0); err != nil {
		return err
	}
	if err := wire(delayID, winID, 1); err != nil {
		return err
	}
	if err := wire(wexitID, winID, 2); err != nil {
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
