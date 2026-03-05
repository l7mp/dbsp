package operator

import (
	"encoding/json"
	"sync"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/internal/logger"
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
	mu     sync.RWMutex
	val    zset.ZSet
	logger logr.Logger
}

// NewInput creates a new InputOp.
func NewInput(opts ...Option) *InputOp {
	o := &InputOp{val: zset.New()}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

func (o *InputOp) Kind() Kind              { return KindInput }
func (o *InputOp) String() string          { return "Input" }
func (o *InputOp) Arity() int              { return 0 }
func (o *InputOp) Linearity() Linearity    { return Primitive }
func (o *InputOp) setLogger(l logr.Logger) { o.logger = l }

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

// MarshalJSON implements json.Marshaler.
func (o *InputOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "input"})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *InputOp) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	o.val = zset.New()
	return nil
}

// OutputOp is a circuit-output boundary operator (arity 1, Primitive).
// Apply(in) returns in unchanged; the executor collects values[id] from the output nodes.
// Set is a no-op since output state flows in via Apply.
type OutputOp struct {
	logger logr.Logger
}

// NewOutput creates a new OutputOp.
func NewOutput(opts ...Option) *OutputOp {
	o := &OutputOp{}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

func (o *OutputOp) Kind() Kind              { return KindOutput }
func (o *OutputOp) String() string          { return "Output" }
func (o *OutputOp) Arity() int              { return 1 }
func (o *OutputOp) Linearity() Linearity    { return Primitive }
func (o *OutputOp) setLogger(l logr.Logger) { o.logger = l }
func (o *OutputOp) Set(_ zset.ZSet)         {}

// Apply returns in unchanged.
func (o *OutputOp) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	return inputs[0], nil
}

// MarshalJSON implements json.Marshaler.
func (o *OutputOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "output"})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *OutputOp) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	return nil
}

// DelayOp is the emit half of a z⁻¹ delay (arity 0, Primitive).
// Apply() returns the value stored by the paired DelayAbsorbOp in the previous timestep.
// Set(v) pre-seeds the delay with v (e.g., for initialization or reset).
type DelayOp struct {
	s      *delayState
	logger logr.Logger
}

// DelayAbsorbOp is the absorb half of a z⁻¹ delay (arity 1, Primitive).
// Apply(in) stores in into shared state for the next timestep's DelayOp.Apply().
// Set is a no-op since state flows in via Apply.
type DelayAbsorbOp struct {
	s      *delayState
	logger logr.Logger
}

// NewDelay creates a paired (DelayOp, DelayAbsorbOp) sharing internal state,
// implementing the z⁻¹ operator. The DelayOp emits the previous stored value;
// the DelayAbsorbOp absorbs the current value for the next timestep.
func NewDelay(opts ...Option) (*DelayOp, *DelayAbsorbOp) {
	s := &delayState{val: zset.New()}
	emit := &DelayOp{s: s}
	absorb := &DelayAbsorbOp{s: s}
	for _, opt := range opts {
		opt.apply(emit)
		opt.apply(absorb)
	}
	emit.logger = logger.NormalizeLogger(emit.logger)
	absorb.logger = logger.NormalizeLogger(absorb.logger)
	return emit, absorb
}

// DelayOp methods.

func (o *DelayOp) Kind() Kind              { return KindDelay }
func (o *DelayOp) String() string          { return "z⁻¹" }
func (o *DelayOp) Arity() int              { return 0 }
func (o *DelayOp) Linearity() Linearity    { return Primitive }
func (o *DelayOp) setLogger(l logr.Logger) { o.logger = l }

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

// MarshalJSON implements json.Marshaler.
func (o *DelayOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "delay"})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *DelayOp) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	if o.s == nil {
		o.s = &delayState{val: zset.New()}
	}
	return nil
}

// DelayAbsorbOp methods.

func (o *DelayAbsorbOp) Kind() Kind              { return KindDelayAbsorb }
func (o *DelayAbsorbOp) String() string          { return "z⁻¹(absorb)" }
func (o *DelayAbsorbOp) Arity() int              { return 1 }
func (o *DelayAbsorbOp) Linearity() Linearity    { return Primitive }
func (o *DelayAbsorbOp) setLogger(l logr.Logger) { o.logger = l }
func (o *DelayAbsorbOp) Set(_ zset.ZSet)         {}

// Apply stores in for the next timestep's DelayOp and returns in unchanged.
func (o *DelayAbsorbOp) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	in := inputs[0]
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	o.s.val = in
	return in, nil
}

// MarshalJSON implements json.Marshaler. Absorb nodes are not serialized directly.
func (o *DelayAbsorbOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "delay_absorb"})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *DelayAbsorbOp) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	if o.s == nil {
		o.s = &delayState{val: zset.New()}
	}
	return nil
}

// IntegrateOp computes the running sum ∫ (arity 1, Bypass).
// Apply(in) → acc += in; returns acc.
// Set(v) initializes the accumulator to v.
// Linearity is Bypass: ∫^Δ = identity.
type IntegrateOp struct {
	mu     sync.Mutex
	acc    zset.ZSet
	logger logr.Logger
}

// NewIntegrate creates a new IntegrateOp.
func NewIntegrate(opts ...Option) *IntegrateOp {
	o := &IntegrateOp{acc: zset.New()}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

func (o *IntegrateOp) Kind() Kind              { return KindIntegrate }
func (o *IntegrateOp) String() string          { return "∫" }
func (o *IntegrateOp) Arity() int              { return 1 }
func (o *IntegrateOp) Linearity() Linearity    { return Bypass }
func (o *IntegrateOp) setLogger(l logr.Logger) { o.logger = l }

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

// MarshalJSON implements json.Marshaler.
func (o *IntegrateOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "integrate"})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *IntegrateOp) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	o.acc = zset.New()
	return nil
}

// DifferentiateOp computes D: output = current - previous (arity 1, Bypass).
// Apply(in) → out = in - prev; prev = in.Clone(); returns out.
// Set(v) initializes prev to v.
// Linearity is Bypass: D^Δ = identity.
type DifferentiateOp struct {
	mu     sync.Mutex
	prev   zset.ZSet
	logger logr.Logger
}

// NewDifferentiate creates a new DifferentiateOp.
func NewDifferentiate(opts ...Option) *DifferentiateOp {
	o := &DifferentiateOp{prev: zset.New()}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

func (o *DifferentiateOp) Kind() Kind              { return KindDifferentiate }
func (o *DifferentiateOp) String() string          { return "D" }
func (o *DifferentiateOp) Arity() int              { return 1 }
func (o *DifferentiateOp) Linearity() Linearity    { return Bypass }
func (o *DifferentiateOp) setLogger(l logr.Logger) { o.logger = l }

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

// MarshalJSON implements json.Marshaler.
func (o *DifferentiateOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "differentiate"})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *DifferentiateOp) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	o.prev = zset.New()
	return nil
}

// Delta0Op outputs its input on the first call, then outputs an empty Z-set (arity 1, Primitive).
// Set(ZSet) re-arms the operator (resets fired = false; the argument is ignored).
// Linearity is Primitive: δ₀^Δ = δ₀.
type Delta0Op struct {
	mu     sync.Mutex
	fired  bool
	logger logr.Logger
}

// NewDelta0 creates a new Delta0Op.
func NewDelta0(opts ...Option) *Delta0Op {
	o := &Delta0Op{}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

func (o *Delta0Op) Kind() Kind              { return KindDelta0 }
func (o *Delta0Op) String() string          { return "δ₀" }
func (o *Delta0Op) Arity() int              { return 1 }
func (o *Delta0Op) Linearity() Linearity    { return Primitive }
func (o *Delta0Op) setLogger(l logr.Logger) { o.logger = l }

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

// MarshalJSON implements json.Marshaler.
func (o *Delta0Op) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "delta0"})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *Delta0Op) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	return nil
}
