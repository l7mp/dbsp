package operator

import (
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/dbsp/zset"
)

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
	KindDistinct      // Distinct: set conversion.
	KindDistinctKeyed // DistinctKeyed: SotW distinct_π.
	KindHKeyed        // HKeyed: incremental distinct_π.
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
	case KindDistinctKeyed:
		return "distinct_keyed"
	case KindHKeyed:
		return "hkeyed"
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

// Reset resets op to its zero state by calling op.Set(zset.New()).
func Reset(op Operator) { op.Set(zset.New()) }
