package operator

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/l7mp/dbsp/engine/zset"
)

// Switch passes its input through while closed and emits the empty Z-set
// while open: the manual switch of a block diagram. Its position is a
// control the host sets, not an input of the circuit, so it selects
// between two circuits, each computable on its own, and a step sees the
// position the host set last. Both positions are linear maps, the identity
// and zero, so incrementalization leaves the switch in place and it is
// the same block on either side of the boundary.
type Switch struct {
	linearOp
	open atomic.Bool
}

// NewSwitch creates a switch in the given position: closed passes the
// input through, open emits nothing.
func NewSwitch(closed bool, opts ...Option) *Switch {
	s := &Switch{linearOp: newLinearOp(KindSwitch, 1, "Switch", opts)}
	s.open.Store(!closed)
	return s
}

// SetClosed sets the position; the next step sees it.
func (o *Switch) SetClosed(closed bool) { o.open.Store(!closed) }

// Closed reports the position.
func (o *Switch) Closed() bool { return !o.open.Load() }

// Apply implements Operator.
func (o *Switch) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	if o.open.Load() {
		return zset.New(), nil
	}
	return inputs[0], nil
}

// MarshalJSON implements json.Marshaler: the position travels as "open",
// absent for a closed switch.
func (o *Switch) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "switch", Open: o.open.Load()})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *Switch) UnmarshalJSON(data []byte) error {
	op, err := UnmarshalOperator(data)
	if err != nil {
		return err
	}
	s, ok := op.(*Switch)
	if !ok {
		return fmt.Errorf("unmarshal switch: got %T", op)
	}
	o.linearOp = s.linearOp
	o.open.Store(s.open.Load())
	return nil
}
