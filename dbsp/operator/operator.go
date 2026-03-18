package operator

import (
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/internal/logger"
)

// Operator is a computation on Z-sets.
type Operator interface {
	fmt.Stringer
	json.Marshaler
	json.Unmarshaler

	// Kind returns the operator's type identifier.
	Kind() Kind

	// Arity returns the number of inputs.
	Arity() int

	// Linearity returns the operator's linearity classification.
	Linearity() Linearity

	// Set initializes or resets the operator's internal state to v.
	// Stateless operators implement this as a no-op.
	// Set(zset.New()) is equivalent to a full reset.
	Set(v zset.ZSet)

	// Apply executes the operator on the given inputs.
	Apply(inputs ...zset.ZSet) (zset.ZSet, error)
}

// loggerSetter is implemented by operators that accept a logger.
type loggerSetter interface {
	setLogger(logr.Logger)
}

// Option configures an operator.
type Option interface {
	apply(loggerSetter)
}

type loggerOption struct {
	logger logr.Logger
}

func (o loggerOption) apply(op loggerSetter) {
	op.setLogger(o.logger)
}

// WithLogger sets the logger for an operator.
func WithLogger(logger logr.Logger) Option {
	return loggerOption{logger: logger}
}

// Kind identifies the concrete type of an operator.
// It is used for type-safe dispatch in switch statements across the circuit,
// executor, and transform packages.
type Kind int

const (
	// Primitive boundary and structural operators.
	KindInput         Kind = iota // InputOp: circuit input boundary.
	KindOutput                    // OutputOp: circuit output boundary.
	KindDelay                     // DelayOp: z⁻¹ emit half.
	KindDelayAbsorb               // DelayAbsorbOp: z⁻¹ absorb half.
	KindIntegrate                 // IntegrateOp: running sum ∫.
	KindDifferentiate             // DifferentiateOp: difference D.
	KindDelta0                    // Delta0Op: initial value injection δ₀.

	// Linear operators (O^Δ = O).
	KindNegate            // Negate: -Z.
	KindLinearCombination // LinearCombination: Σ coeffs[i]·inputs[i].  Covers Sum and Subtract.
	KindSelect            // Select: σ filter.
	KindProject           // Project: π projection.
	KindUnwind            // Unwind: array flatten.

	// Bilinear operator (three-term expansion).
	KindCartesian // CartesianProduct: A × B.

	// Non-linear operators (D ∘ O ∘ ∫ wrapping).
	KindDistinct  // Distinct: set conversion.
	KindAggregate // Aggregate: generic incremental GROUP BY aggregate.
)

// String returns a human-readable name for the operator kind.
func (k Kind) String() string {
	switch k {
	case KindInput:
		return "input"
	case KindOutput:
		return "output"
	case KindDelay:
		return "delay"
	case KindDelayAbsorb:
		return "delay_absorb"
	case KindIntegrate:
		return "integrate"
	case KindDifferentiate:
		return "differentiate"
	case KindDelta0:
		return "delta0"
	case KindNegate:
		return "negate"
	case KindLinearCombination:
		return "linear_combination"
	case KindSelect:
		return "select"
	case KindProject:
		return "project"
	case KindUnwind:
		return "unwind"
	case KindCartesian:
		return "cartesian"
	case KindDistinct:
		return "distinct"
	case KindAggregate:
		return "aggregate_keyed"
	default:
		return "unknown"
	}
}

// Linearity classifies operators for incrementalization.
type Linearity int

const (
	// Linear operators satisfy O(A + B) = O(A) + O(B).
	// Their incremental version is identical: O^Δ = O.
	Linear Linearity = iota

	// Bilinear operators are linear in each argument separately.
	// They require the three-term expansion for incrementalization.
	Bilinear

	// NonLinear operators are neither linear nor bilinear.
	// They require D ∘ O ∘ ∫ wrapping for incrementalization.
	NonLinear

	// Bypass operators are replaced by identity in the incremental circuit.
	// Used by IntegrateOp and DifferentiateOp: ∫^Δ = I and D^Δ = I.
	Bypass

	// Primitive operators pass through unchanged into the incremental circuit.
	// Used by InputOp, OutputOp, Delta0Op.
	Primitive
)

// String returns a string representation of linearity.
func (l Linearity) String() string {
	switch l {
	case Linear:
		return "Linear"
	case Bilinear:
		return "Bilinear"
	case NonLinear:
		return "NonLinear"
	case Bypass:
		return "Bypass"
	case Primitive:
		return "Primitive"
	default:
		return "Unknown"
	}
}

// Reset resets op to its zero state by calling op.Set(zset.New()).
func Reset(op Operator) { op.Set(zset.New()) }

// baseOp is embedded by every operator to provide a shared logger, a no-op
// Set implementation for stateless operators, and JSON marshal/unmarshal via
// the embedded jsonOp (which carries the wire type string).
// Operators with stateful Set override it; operators with extra JSON fields
// override MarshalJSON/UnmarshalJSON.
type baseOp struct {
	jsonOp
	logger logr.Logger
}

func (b *baseOp) setLogger(l logr.Logger) { b.logger = l }
func (b *baseOp) Set(_ zset.ZSet)         {}

// newBaseOp returns a baseOp initialised with the given wire type string and
// options applied.
func newBaseOp(opType string, opts []Option) baseOp {
	b := baseOp{jsonOp: jsonOp{Type: opType}}
	for _, opt := range opts {
		opt.apply(&b)
	}
	b.logger = logger.NormalizeLogger(b.logger)
	return b
}

// opBase extends baseOp with the metadata that every concrete operator carries:
// its Kind, arity, display string, and Linearity. Promoted methods implement
// the corresponding Operator interface methods so concrete types only need to
// provide Apply (and, where necessary, Set and JSON marshal/unmarshal).
type opBase struct {
	baseOp
	kind      Kind
	arity     int
	display   string
	linearity Linearity
}

func (o *opBase) String() string       { return o.display }
func (o *opBase) Arity() int           { return o.arity }
func (o *opBase) Linearity() Linearity { return o.linearity }
func (o *opBase) Kind() Kind           { return o.kind }

func newOpBase(kind Kind, arity int, display string, lin Linearity, opts []Option) opBase {
	return opBase{
		baseOp:    newBaseOp(kind.String(), opts),
		kind:      kind,
		arity:     arity,
		display:   display,
		linearity: lin,
	}
}

// linearOp, bilinearOp, and nonLinearOp are thin wrappers around opBase that
// fix the linearity at construction time. Concrete operator types embed the
// appropriate wrapper so their struct declaration documents the linearity class.
type linearOp struct{ opBase }
type bilinearOp struct{ opBase }
type nonLinearOp struct{ opBase }

func newLinearOp(kind Kind, arity int, display string, opts []Option) linearOp {
	return linearOp{newOpBase(kind, arity, display, Linear, opts)}
}

func newBilinearOp(kind Kind, arity int, display string, opts []Option) bilinearOp {
	return bilinearOp{newOpBase(kind, arity, display, Bilinear, opts)}
}

func newNonLinearOp(kind Kind, arity int, display string, opts []Option) nonLinearOp {
	return nonLinearOp{newOpBase(kind, arity, display, NonLinear, opts)}
}
