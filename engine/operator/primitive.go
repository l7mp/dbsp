package operator

import (
	"fmt"
	"sync"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/zset"
)

// delayState is the shared storage between a paired DelayOp (emit) and
// DelayAbsorbOp (absorb): a ring of k slots implementing z⁻ᵏ. Each step the
// emit half returns vals[pos] (the value absorbed k steps ago) and the absorb
// half then overwrites vals[pos] with the current value and advances pos, so
// the delay costs O(1) per step at any k. Slot contents total the in-flight
// values, independent of k; empty slots hold empty Z-sets.
type delayState struct {
	mu   sync.RWMutex
	vals []zset.ZSet
	pos  int
}

func newDelayState(k int) *delayState {
	if k < 1 {
		k = 1
	}
	vals := make([]zset.ZSet, k)
	for i := range vals {
		vals[i] = zset.New()
	}
	return &delayState{vals: vals}
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
func (o *InputOp) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
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
func (o *OutputOp) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
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

// NewDelay creates a paired (DelayOp, DelayAbsorbOp) sharing a k-slot ring,
// implementing the z⁻ᵏ operator: the DelayOp emits the value the
// DelayAbsorbOp absorbed k timesteps earlier. k < 1 is clamped to 1; k = 1 is
// the plain z⁻¹.
func NewDelay(k int, opts ...Option) (*DelayOp, *DelayAbsorbOp) {
	if k < 1 {
		k = 1
	}
	s := newDelayState(k)
	emit := &DelayOp{baseOp: newBaseOp("delay", opts), s: s}
	if k > 1 {
		emit.jsonOp.K = k
	}
	return emit, &DelayAbsorbOp{baseOp: newBaseOp("delay_absorb", opts), s: s}
}

// DelayOp methods.

func (o *DelayOp) Kind() Kind { return KindDelay }

// K returns the delay depth in timesteps.
func (o *DelayOp) K() int { return len(o.s.vals) }

func (o *DelayOp) String() string {
	if o.K() == 1 {
		return "z⁻¹"
	}
	return fmt.Sprintf("z⁻%d", o.K())
}

func (o *DelayOp) Arity() int           { return 0 }
func (o *DelayOp) Linearity() Linearity { return Primitive }

// Set clears the ring and pre-seeds the value emitted next (e.g., after reset).
func (o *DelayOp) Set(v zset.ZSet) {
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	for i := range o.s.vals {
		o.s.vals[i] = zset.New()
	}
	o.s.pos = 0
	o.s.vals[0] = v
}

// Apply returns the value stored by the DelayAbsorbOp k timesteps ago.
func (o *DelayOp) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	o.s.mu.RLock()
	defer o.s.mu.RUnlock()
	return o.s.vals[o.s.pos], nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *DelayOp) UnmarshalJSON(data []byte) error {
	if err := o.baseOp.UnmarshalJSON(data); err != nil {
		return err
	}
	o.s = newDelayState(o.jsonOp.K)
	return nil
}

// DelayAbsorbOp methods.

func (o *DelayAbsorbOp) Kind() Kind { return KindDelayAbsorb }

func (o *DelayAbsorbOp) String() string {
	if len(o.s.vals) == 1 {
		return "z⁻¹(absorb)"
	}
	return fmt.Sprintf("z⁻%d(absorb)", len(o.s.vals))
}

func (o *DelayAbsorbOp) Arity() int           { return 1 }
func (o *DelayAbsorbOp) Linearity() Linearity { return Primitive }

// Apply stores in into the ring slot its paired DelayOp just emitted from,
// advances the ring, and returns in unchanged. The executor's topological
// order runs the emit half before the absorb half within a step, so the slot
// being overwritten has already been emitted.
func (o *DelayAbsorbOp) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	in := inputs[0]
	o.s.mu.Lock()
	defer o.s.mu.Unlock()
	o.s.vals[o.s.pos] = in
	o.s.pos = (o.s.pos + 1) % len(o.s.vals)
	return in, nil
}

// UnmarshalJSON implements json.Unmarshaler. Absorb nodes are not serialized
// directly but the method is provided for completeness.
func (o *DelayAbsorbOp) UnmarshalJSON(data []byte) error {
	if o.s == nil {
		o.s = newDelayState(1)
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

// Apply folds in into the accumulator in place and returns the running sum:
// O(|in|) per step, no state copy (Insert allocates fresh entry structs, so
// nothing of the input container is retained).
//
// The emitted accumulator is step-scoped: the same container is advanced in
// place on the next step, so consumers must not retain it across steps.
// Consumers that need the previous integral must not read the accumulator
// through a plain reference-holding delay (the value would advance under
// the delay); delay the deltas BEFORE integrating instead (z⁻¹ then ∫, as
// the incrementalizer wires it; the two commute).
func (o *IntegrateOp) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	in := inputs[0]
	o.mu.Lock()
	defer o.mu.Unlock()
	if in.IsZero() {
		return o.acc, nil
	}
	in.Iter(func(doc datamodel.Document, w zset.Weight) bool {
		o.acc.Insert(doc, w)
		return true
	})
	if o.logger.V(2).Enabled() {
		o.logger.V(2).Info("operator", "op", o.String(), "input", in.String(), "acc", o.acc.String())
	}
	return o.acc, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *IntegrateOp) UnmarshalJSON(data []byte) error {
	o.acc = zset.New()
	return o.baseOp.UnmarshalJSON(data)
}

// DifferentiateOp computes D: output = current - previous (arity 1, Bypass).
// Apply(in) → out = in - prev; prev = in.ShallowCopy(); returns out.
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
func (o *DifferentiateOp) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	in := inputs[0]
	o.mu.Lock()
	defer o.mu.Unlock()
	out := in.Subtract(o.prev)
	o.prev = in.ShallowCopy()
	if o.logger.V(2).Enabled() {
		o.logger.V(2).Info("operator", "op", o.String(), "input", in.String(), "output", out.String())
	}
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
func (o *Delta0Op) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fired {
		return zset.New(), nil
	}
	o.fired = true
	return inputs[0], nil
}

// DistinctH detects whether the multiplicity of an element in the input set is changing from
// negative to positive or vice-versa (arity 1, Primitive).
type DistinctH struct{ baseOp }

// NewDistinctH creates a new DistinctH operator.
func NewDistinctH(opts ...Option) *DistinctH {
	return &DistinctH{newBaseOp("H_func", opts)}
}

func (o *DistinctH) Kind() Kind           { return KindDistinctH }
func (o *DistinctH) String() string       { return "H_func" }
func (o *DistinctH) Arity() int           { return 2 }
func (o *DistinctH) Linearity() Linearity { return Primitive }

// Set re-arms the operator (resets fired .
func (o *DistinctH) Set(_ zset.ZSet) {}

// Apply implements Operator.
func (o *DistinctH) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	prev := inputs[0] // z⁻¹(I(δ)) = integrated state before this step
	delta := inputs[1]
	result := zset.New()
	delta.Iter(func(elem datamodel.Document, w zset.Weight) bool {
		oldWeight := prev.Lookup(elem.Hash())
		newWeight := oldWeight + w
		if oldWeight <= 0 && newWeight > 0 {
			result.Insert(elem, 1)
		} else if oldWeight > 0 && newWeight <= 0 {
			result.Insert(elem, -1)
		}
		return true
	})
	return result, nil
}
