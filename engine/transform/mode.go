package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

// Enabled is the initial position of a closed loop's switch: a constructor
// argument of New for the Reconciler, the SmithPredictor and the
// DualRateSmith, the "enabled" field of their serialized entries (absent
// means enabled). Disabled, the loop runs in manual mode: its output is
// the open-loop delta, and the jacket keeps integrating the outstanding
// correction without emitting it. The host flips the switch at run time
// through the runtime (Circuit.SetEnabled); closing the loop then emits
// exactly what the plant has not confirmed by then, once. This is
// manual/automatic mode with bumpless transfer, the classical anti-windup
// provision for a controller that must not correct while the plant is
// still being loaded.
type Enabled bool

// LoopPrefix returns the node-id prefix of a loop transform's jacket, and
// whether the type is a loop at all: the switch of a loop is the node
// <prefix><output>_switch.
func LoopPrefix(typ TransformerType) (string, bool) {
	switch typ {
	case Reconciler:
		return "_rec_", true
	case SmithPredictor:
		return "_smith_", true
	case DualRateSmith:
		return "_drs_", true
	}
	return "", false
}

// injectModeSwitch wires the output of a loop as
//
//	out = δD + S(z⁻¹U − δS),
//
// where predID carries the open-loop delta δD, delayID the delayed
// correction z⁻¹U, feedbackID the delta the jacket subtracts (the raw
// feedback for the Reconciler, the prediction delta for the Smith
// variants) and S is the loop's switch (operator.Switch, the node
// <prefix>_switch). Closed, the switch passes the pending correction and
// the output is U; open, it passes nothing and the output is δD. The
// switch is linear in both positions, so the Incrementalizer leaves the
// wiring alone wherever the jacket is injected.
func injectModeSwitch(c *circuit.Circuit, prefix, kind, predID, delayID, feedbackID string, closed bool, outputID string) error {
	pendID := prefix + "_pend"
	switchID := prefix + "_switch"
	outID := prefix + "_out"
	if err := c.AddNode(circuit.Op(pendID, operator.NewMinus())); err != nil {
		return fmt.Errorf("%s: add pending node: %w", kind, err)
	}
	if err := c.AddNode(circuit.Op(switchID, operator.NewSwitch(closed))); err != nil {
		return fmt.Errorf("%s: add switch: %w", kind, err)
	}
	if err := c.AddNode(circuit.Op(outID, operator.NewPlus())); err != nil {
		return fmt.Errorf("%s: add output node: %w", kind, err)
	}
	for _, w := range []struct {
		from, to string
		port     int
	}{
		{delayID, pendID, 0},
		{feedbackID, pendID, 1},
		{pendID, switchID, 0},
		{predID, outID, 0},
		{switchID, outID, 1},
		{outID, outputID, 0},
	} {
		if err := c.AddEdge(circuit.NewEdge(w.from, w.to, w.port)); err != nil {
			return fmt.Errorf("%s: wire %s to %s: %w", kind, w.from, w.to, err)
		}
	}
	return nil
}
