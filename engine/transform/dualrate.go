package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

// dualRateSmith is the Smith dead-time compensator with its compensation
// window retimed to a wall clock (doc/pres/smith.org, "Dual-rate
// retirement"). Commands enter the window on the event clock and leave it
// on a tick stream g, so retirement is aligned in wall time: with tick
// period T the window is W = k·T seconds at any event rate. Per reconciled
// pair the transform injects
//
//	U  = ∫(δD − δS)                  (the pending correction, emitted)
//	δS = dist^Δ(δY + z⁻¹U − E)       (the prediction delta)
//
// where E is the release stream of k transfer cells chained on the entry
// tap. Each cell holds what entered until the next tick releases it,
//
//	H = ∫(X − G),   G = (z⁻¹H) × g,
//
// with the gate a per-step cartesian product against the tick delta: the
// tick must be a single +1 assertion of an empty document per period (the
// misc Tick source's pulse mode), so document merge passes the gated
// commands through content-identical and nothing integrates g.
//
// The transform is self-contained: it injects its own tick input node
// (input_<tick>, shared by every pair of one application), and the commit
// path subscribes transform-injected inputs by their topic stem, so the
// host contributes nothing but the tick source. Like the SmithPredictor,
// the transform runs on the delta side of the chain, after the
// Incrementalizer; the gate's product is a per-step block of the
// incremental form and must never be expanded by incrementalization.
type dualRateSmith struct {
	k     int
	tick  string
	pairs []ReconcilerPair
}

// DefaultTickInput is the logical name of the clock input the DualRateSmith
// injects when the transform names none.
const DefaultTickInput = "Tick"

// NewDualRateSmith creates a dual-rate Smith compensator transform with a
// compensation window of k ticks for the given input/output pairs. tick is
// the logical name of the clock input (empty selects DefaultTickInput).
// With no pairs, self-referential pairs are auto-detected exactly as for
// the Reconciler.
func NewDualRateSmith(k int, tick string, pairs ...ReconcilerPair) Transformer {
	if tick == "" {
		tick = DefaultTickInput
	}
	return &dualRateSmith{k: k, tick: tick, pairs: pairs}
}

func (t *dualRateSmith) Name() TransformerType { return DualRateSmith }

func (t *dualRateSmith) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	if t.k < 1 {
		return nil, fmt.Errorf("dualrate: compensation window k is %d ticks, need at least 1", t.k)
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
			return nil, fmt.Errorf("dualrate: input %q used in multiple pairs", p.InputID)
		}
		if seenOutputs[p.OutputID] {
			return nil, fmt.Errorf("dualrate: output %q used in multiple pairs", p.OutputID)
		}
		seenInputs[p.InputID] = true
		seenOutputs[p.OutputID] = true
	}

	// The clock input, shared by every pair's gates. An existing input node
	// of that name is reused (the host may declare the stream explicitly).
	tickID := circuit.InputNodeID(t.tick)
	if n := clone.Node(tickID); n == nil {
		if err := clone.AddNode(circuit.Input(tickID)); err != nil {
			return nil, fmt.Errorf("dualrate: add tick input: %w", err)
		}
	} else if n.Kind() != operator.KindInput {
		return nil, fmt.Errorf("dualrate: tick node %q is %s, not input", tickID, n.Kind())
	}

	for _, p := range pairs {
		if err := injectDualRateLoop(clone, p, t.k, tickID); err != nil {
			return nil, err
		}
	}

	return clone, nil
}

func injectDualRateLoop(c *circuit.Circuit, pair ReconcilerPair, k int, tickID string) error {
	prefix := "_drs_" + pair.OutputID
	core, err := injectPredictorCore(c, pair, prefix, "dualrate")
	if err != nil {
		return err
	}

	// The tick-domain window: k transfer cells chained on the entry tap
	// z⁻¹U. Cell i holds everything that entered since the last tick
	// (H = X + z⁻¹H − G, the plus/z⁻¹ realization of ∫(X − G)) and
	// releases it on the tick (G = z⁻¹H gated by g). The last release
	// stream is the window exit E.
	wire := func(from, to string, port int) error {
		if err := c.AddEdge(circuit.NewEdge(from, to, port)); err != nil {
			return fmt.Errorf("dualrate: wire %s to %s: %w", from, to, err)
		}
		return nil
	}
	x := core.delayID
	for i := 1; i <= k; i++ {
		aID := fmt.Sprintf("%s_a%d", prefix, i)
		zaID := fmt.Sprintf("%s_za%d", prefix, i)
		gID := fmt.Sprintf("%s_g%d", prefix, i)
		if err := c.AddNode(circuit.Op(aID, operator.NewLinearCombination([]int{1, 1, -1}))); err != nil {
			return fmt.Errorf("dualrate: add cell hold %d: %w", i, err)
		}
		if err := c.AddNode(circuit.Delay(zaID, 1)); err != nil {
			return fmt.Errorf("dualrate: add cell delay %d: %w", i, err)
		}
		if err := c.AddNode(circuit.Op(gID, operator.NewCartesianProduct())); err != nil {
			return fmt.Errorf("dualrate: add cell gate %d: %w", i, err)
		}
		for _, w := range []struct {
			from, to string
			port     int
		}{
			{x, aID, 0},
			{zaID, aID, 1},
			{gID, aID, 2},
			{aID, zaID, 0},
			{zaID, gID, 0},
			{tickID, gID, 1},
		} {
			if err := wire(w.from, w.to, w.port); err != nil {
				return err
			}
		}
		x = gID
	}
	return wire(x, core.winID, 2)
}
