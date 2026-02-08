// Package operator defines the Operator interface and implementations for DBSP.
package operator

import "github.com/l7mp/dbsp/zset"

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
	default:
		return "Unknown"
	}
}

// Operator is a computation on Z-sets.
type Operator interface {
	// Name returns the operator's display name.
	Name() string

	// Arity returns the number of inputs.
	Arity() int

	// Linearity returns the operator's linearity classification.
	Linearity() Linearity

	// Apply executes the operator on the given inputs.
	Apply(inputs ...zset.ZSet) (zset.ZSet, error)
}
