package operator

import (
	"sync"

	"github.com/l7mp/dbsp/engine/zset"
)

// delayState is the shared storage between a paired DelayOp (emit) and DelayAbsorbOp (absorb).
type delayState struct {
	mu  sync.RWMutex
	val zset.ZSet
}

// InputOp is a circuit-input boundary operator (arity 0, Primitive).
// Set(v) stores v as the value that Apply() will return.
// The executor calls Set before each timestep to inject the circuit input value.
type InputOp struct {
	baseOp
	mu  sync.RWMutex
	val zset.ZSet
}

// NewInput creates a new InputOp.
func NewInput(opts ...Option) *InputOp {
	return &InputOp{baseOp: newBaseOp("input", opts), val: zset.New()}
}

func (o *InputOp) Kind() Kind           { return KindInput }
func (o *InputOp) String() string       { return "Input" }
func (o *InputOp) Arity() int           { return 0 }
func (o *InputOp) Linearity() Linearity { return Primitive }

// Set stores v; Apply returns this value until the next Set call.
func (o *InputOp) Set(v zset.ZSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.val = v
}

// Apply returns the stored value.
func (o *InputOp) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.val, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *InputOp) UnmarshalJSON(data []byte) error {
	o.val = zset.New()
	return o.baseOp.UnmarshalJSON(data)
}

// OutputOp is a circuit-output boundary operator (Primitive).
// Apply sums all incoming inputs. With one input, this is identity.
type OutputOp struct {
	baseOp
}

// NewOutput creates a new OutputOp.
func NewOutput(opts ...Option) *OutputOp {
	return &OutputOp{baseOp: newBaseOp("output", opts)}
}

func (o *OutputOp) Kind() Kind           { return KindOutput }
func (o *OutputOp) String() string       { return "Output" }
func (o *OutputOp) Arity() int           { return 1 }
func (o *OutputOp) Linearity() Linearity { return Primitive }

// Apply returns the sum of all inputs.
func (o *OutputOp) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	if len(inputs) == 0 {
		return zset.New(), nil
	}
	out := zset.New()
	for _, in := range inputs {
		out = out.Add(in)
	}
	return out, nil
}

// DelayOp is the emit half of a z⁻¹ delay (arity 0, Primitive).
// Apply() returns the value stored by the paired DelayAbsorbOp in the previous timestep.
// Set(v) pre-seeds the delay with v (e.g., for initialization or reset).
type DelayOp struct {
	baseOp
	s *delayState
}

// DelayAbsorbOp is the absorb half of a z⁻¹ delay (arity 1, Primitive).
// Apply(in) stores in into shared state for the next timestep's DelayOp.Apply().
type DelayAbsorbOp struct {
	baseOp
	s *delayState
}

// NewDelay creates a paired (DelayOp, DelayAbsorbOp) sharing internal state,
// implementing the z⁻¹ operator. The DelayOp emits the previous stored value;
// the DelayAbsorbOp absorbs the current value for the next timestep.
func NewDelay(opts ...Option) (*DelayOp, *DelayAbsorbOp) {
	s := &delayState{val: zset.New()}
	return &DelayOp{baseOp: newBaseOp("delay", opts), s: s},
		&DelayAbsorbOp{baseOp: newBaseOp("delay_absorb", opts), s: s}
}

// DelayOp methods.

func (o *DelayOp) Kind() Kind           { return KindDelay }
func (o *DelayOp) String() string       { return "z⁻¹" }
func (o *DelayOp) Arity() int           { return 0 }
func (o *DelayOp) Linearity() Linearity { return Primitive }

// Set pre-seeds the stored value with v (e.g., after reset).
func (o *DelayOp) Set(v zset.ZSet) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	o.s.val = v
}

// Apply returns the value stored by the previous timestep's DelayAbsorbOp.
func (o *DelayOp) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	o.s.mu.RLock()
	defer o.s.mu.RUnlock()
	return o.s.val, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *DelayOp) UnmarshalJSON(data []byte) error {
	if o.s == nil {
		o.s = &delayState{val: zset.New()}
	}
	return o.baseOp.UnmarshalJSON(data)
}

// DelayAbsorbOp methods.

func (o *DelayAbsorbOp) Kind() Kind           { return KindDelayAbsorb }
func (o *DelayAbsorbOp) String() string       { return "z⁻¹(absorb)" }
func (o *DelayAbsorbOp) Arity() int           { return 1 }
func (o *DelayAbsorbOp) Linearity() Linearity { return Primitive }

// Apply stores in for the next timestep's DelayOp and returns in unchanged.
func (o *DelayAbsorbOp) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	in := inputs[0]
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	o.s.val = in
	return in, nil
}

// UnmarshalJSON implements json.Unmarshaler. Absorb nodes are not serialized
// directly but the method is provided for completeness.
func (o *DelayAbsorbOp) UnmarshalJSON(data []byte) error {
	if o.s == nil {
		o.s = &delayState{val: zset.New()}
	}
	return o.baseOp.UnmarshalJSON(data)
}

// IntegrateOp computes the running sum ∫ (arity 1, Bypass).
// Apply(in) → acc += in; returns acc.
// Set(v) initializes the accumulator to v.
// Linearity is Bypass: ∫^Δ = identity.
type IntegrateOp struct {
	baseOp
	mu  sync.Mutex
	acc zset.ZSet
}

// NewIntegrate creates a new IntegrateOp.
func NewIntegrate(opts ...Option) *IntegrateOp {
	return &IntegrateOp{baseOp: newBaseOp("integrate", opts), acc: zset.New()}
}

func (o *IntegrateOp) Kind() Kind           { return KindIntegrate }
func (o *IntegrateOp) String() string       { return "∫" }
func (o *IntegrateOp) Arity() int           { return 1 }
func (o *IntegrateOp) Linearity() Linearity { return Bypass }

// Set initializes the accumulator to v.
func (o *IntegrateOp) Set(v zset.ZSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.acc = v
}

// Apply adds in to the accumulator and returns the running sum.
func (o *IntegrateOp) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	in := inputs[0]
	o.mu.Lock()
	defer o.mu.Unlock()
	o.acc = o.acc.Add(in)
	o.logger.V(2).Info("operator", "op", o.String(), "input", in.String(), "acc", o.acc.String())
	return o.acc, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *IntegrateOp) UnmarshalJSON(data []byte) error {
	o.acc = zset.New()
	return o.baseOp.UnmarshalJSON(data)
}

// DifferentiateOp computes D: output = current - previous (arity 1, Bypass).
// Apply(in) → out = in - prev; prev = in.Clone(); returns out.
// Set(v) initializes prev to v.
// Linearity is Bypass: D^Δ = identity.
type DifferentiateOp struct {
	baseOp
	mu   sync.Mutex
	prev zset.ZSet
}

// NewDifferentiate creates a new DifferentiateOp.
func NewDifferentiate(opts ...Option) *DifferentiateOp {
	return &DifferentiateOp{baseOp: newBaseOp("differentiate", opts), prev: zset.New()}
}

func (o *DifferentiateOp) Kind() Kind           { return KindDifferentiate }
func (o *DifferentiateOp) String() string       { return "D" }
func (o *DifferentiateOp) Arity() int           { return 1 }
func (o *DifferentiateOp) Linearity() Linearity { return Bypass }

// Set initializes prev to v.
func (o *DifferentiateOp) Set(v zset.ZSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.prev = v
}

// Apply returns in - prev, then stores in as the new prev.
func (o *DifferentiateOp) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	in := inputs[0]
	o.mu.Lock()
	defer o.mu.Unlock()
	out := in.Subtract(o.prev)
	o.prev = in.Clone()
	o.logger.V(2).Info("operator", "op", o.String(), "input", in.String(), "output", out.String())
	return out, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *DifferentiateOp) UnmarshalJSON(data []byte) error {
	o.prev = zset.New()
	return o.baseOp.UnmarshalJSON(data)
}

// Delta0Op outputs its input on the first call, then outputs an empty Z-set (arity 1, Primitive).
// Set(ZSet) re-arms the operator (resets fired = false; the argument is ignored).
// Linearity is Primitive: δ₀^Δ = δ₀.
type Delta0Op struct {
	baseOp
	mu    sync.Mutex
	fired bool
}

// NewDelta0 creates a new Delta0Op.
func NewDelta0(opts ...Option) *Delta0Op {
	return &Delta0Op{baseOp: newBaseOp("delta0", opts)}
}

func (o *Delta0Op) Kind() Kind           { return KindDelta0 }
func (o *Delta0Op) String() string       { return "δ₀" }
func (o *Delta0Op) Arity() int           { return 1 }
func (o *Delta0Op) Linearity() Linearity { return Primitive }

// Set re-arms the operator (resets fired to false; v is ignored).
func (o *Delta0Op) Set(_ zset.ZSet) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fired = false
}

// Apply returns inputs[0] on the first call, then zset.New() on subsequent calls.
func (o *Delta0Op) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fired {
		return zset.New(), nil
	}
	o.fired = true
	return inputs[0], nil
}
