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
//
// The transform runs on the delta side of the chain, after the
// Incrementalizer: dead time is a property of the actuated incremental
// loop (a snapshot circuit has no feedback and hence no dead time), so the
// jacket is injected directly in delta form, with dist^Δ placed as its
// compiled shape (z⁻¹ then ∫ feeding the distinct H function).
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

// loopCore names the injected nodes of the desired-state predictor
// skeleton shared by the SmithPredictor and the DualRateSmith: the
// correction integrator U = ∫(δD − δS) with its z⁻¹ entry tap, the
// window sum δY + z⁻¹U − <exit>, and the prediction distinct in compiled
// delta form. The caller wires its window exit into winID port 2.
type loopCore struct {
	subID, accID, delayID, winID string
}

// injectPredictorCore injects the shared predictor skeleton for one pair;
// kind prefixes the error messages ("smith" or "dualrate").
func injectPredictorCore(c *circuit.Circuit, pair ReconcilerPair, prefix, kind string) (loopCore, error) {
	inputNode := c.Node(pair.InputID)
	outputNode := c.Node(pair.OutputID)
	if inputNode == nil {
		return loopCore{}, fmt.Errorf("%s: input node %q not found", kind, pair.InputID)
	}
	if outputNode == nil {
		return loopCore{}, fmt.Errorf("%s: output node %q not found", kind, pair.OutputID)
	}
	if inputNode.Kind() != operator.KindInput {
		return loopCore{}, fmt.Errorf("%s: node %q is %s, not input", kind, pair.InputID, inputNode.Kind())
	}
	if outputNode.Kind() != operator.KindOutput {
		return loopCore{}, fmt.Errorf("%s: node %q is %s, not output", kind, pair.OutputID, outputNode.Kind())
	}
	// One loop per output: refuse to stack on any existing jacket. The
	// Reconciler runs on the snapshot side, so its jacket reaches the
	// delta-side transforms with the incrementalizer's ^Δ suffix; check
	// both spellings.
	for _, existing := range []struct{ node, what string }{
		{"_rec_" + pair.OutputID + "_acc", "Reconciler"},
		{"_rec_" + pair.OutputID + "_acc^Δ", "Reconciler"},
		{"_smith_" + pair.OutputID + "_acc", "Smith"},
		{"_drs_" + pair.OutputID + "_acc", "DualRateSmith"},
	} {
		if c.Node(existing.node) != nil {
			return loopCore{}, fmt.Errorf("%s: output %q already carries a %s loop", kind, pair.OutputID, existing.what)
		}
	}

	inEdges := c.EdgesTo(pair.OutputID)
	if len(inEdges) == 0 {
		return loopCore{}, fmt.Errorf("%s: output node %q has no incoming edges", kind, pair.OutputID)
	}

	core := loopCore{
		subID:   prefix + "_sub",
		accID:   prefix + "_acc",
		delayID: prefix + "_delay",
		winID:   prefix + "_win",
	}
	sumID := prefix + "_sum"
	distID := prefix + "_dist"
	distNoopID := prefix + "_dist_noop"
	distDelayID := prefix + "_dist_delay"
	distIntID := prefix + "_dist_int"

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
			return loopCore{}, fmt.Errorf("%s: add sum node: %w", kind, err)
		}
		for _, e := range inEdges {
			if err := c.AddEdge(circuit.NewEdge(e.From, sumID, e.Port)); err != nil {
				return loopCore{}, fmt.Errorf("%s: wire pred to sum: %w", kind, err)
			}
		}
		predID = sumID
	}

	// U = ∫(δD − δS): the Reconciler's sub/acc/delay feedback, with the
	// prediction delta in place of the raw feedback.
	if err := c.AddNode(circuit.Op(core.subID, operator.NewMinus())); err != nil {
		return loopCore{}, fmt.Errorf("%s: add sub node: %w", kind, err)
	}
	if err := c.AddNode(circuit.Op(core.accID, operator.NewPlus())); err != nil {
		return loopCore{}, fmt.Errorf("%s: add acc node: %w", kind, err)
	}
	if err := c.AddNode(circuit.Delay(core.delayID, 1)); err != nil {
		return loopCore{}, fmt.Errorf("%s: add delay node: %w", kind, err)
	}

	// δS = dist^Δ(δY + z⁻¹U − <exit>): the prediction distinct in its
	// compiled delta form, H(z⁻¹∫, δ), the same shape the incrementalizer
	// emits for a snapshot-side distinct. Its integral is the prediction.
	if err := c.AddNode(circuit.Op(core.winID, operator.NewLinearCombination([]int{1, 1, -1}))); err != nil {
		return loopCore{}, fmt.Errorf("%s: add window sum node: %w", kind, err)
	}
	if err := c.AddNode(circuit.Op(distNoopID, operator.NewNoOp())); err != nil {
		return loopCore{}, fmt.Errorf("%s: add prediction fan node: %w", kind, err)
	}
	if err := c.AddNode(circuit.Delay(distDelayID, 1)); err != nil {
		return loopCore{}, fmt.Errorf("%s: add prediction delay node: %w", kind, err)
	}
	if err := c.AddNode(circuit.Integrate(distIntID)); err != nil {
		return loopCore{}, fmt.Errorf("%s: add prediction integrator node: %w", kind, err)
	}
	if err := c.AddNode(circuit.Op(distID, operator.NewDistinctH())); err != nil {
		return loopCore{}, fmt.Errorf("%s: add prediction distinct node: %w", kind, err)
	}

	for _, e := range inEdges {
		if err := c.RemoveEdge(e.From, pair.OutputID, e.Port); err != nil {
			return loopCore{}, fmt.Errorf("%s: remove pred to output edge: %w", kind, err)
		}
	}

	wire := func(from, to string, port int) error {
		if err := c.AddEdge(circuit.NewEdge(from, to, port)); err != nil {
			return fmt.Errorf("%s: wire %s to %s: %w", kind, from, to, err)
		}
		return nil
	}
	for _, w := range []struct {
		from, to string
		port     int
	}{
		{predID, core.subID, 0},
		{distID, core.subID, 1},
		{core.subID, core.accID, 0},
		{core.delayID, core.accID, 1},
		{core.accID, core.delayID, 0},
		{pair.InputID, core.winID, 0},
		{core.delayID, core.winID, 1},
		{core.winID, distNoopID, 0},
		{distNoopID, distDelayID, 0},
		{distDelayID, distIntID, 0},
		{distIntID, distID, 0},
		{distNoopID, distID, 1},
		{core.accID, pair.OutputID, 0},
	} {
		if err := wire(w.from, w.to, w.port); err != nil {
			return loopCore{}, err
		}
	}

	return core, nil
}

func injectSmithLoop(c *circuit.Circuit, pair ReconcilerPair, k int) error {
	prefix := "_smith_" + pair.OutputID
	core, err := injectPredictorCore(c, pair, prefix, "smith")
	if err != nil {
		return err
	}

	// The window taps: the acc feedback delay doubles as the entry tap
	// z⁻¹U; a single z⁻⁽ᵏ⁻¹⁾ ring delay on it yields the exit tap z⁻ᴷU.
	wexitID := prefix + "_wexit"
	if err := c.AddNode(circuit.Delay(wexitID, k-1)); err != nil {
		return fmt.Errorf("smith: add window exit delay: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(core.delayID, wexitID, 0)); err != nil {
		return fmt.Errorf("smith: wire entry tap to window exit: %w", err)
	}
	if err := c.AddEdge(circuit.NewEdge(wexitID, core.winID, 2)); err != nil {
		return fmt.Errorf("smith: wire window exit: %w", err)
	}
	return nil
}
