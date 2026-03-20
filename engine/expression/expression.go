// Package expr provides a minimal expression interface for DBSP operators.
package expression

import (
	"encoding/json"
	"fmt"
)

// Expression is the minimal interface for data access and transformation.
type Expression interface {
	fmt.Stringer
	json.Marshaler
	json.Unmarshaler

	// Evaluate executes the expression with the given context.
	Evaluate(ctx *EvalContext) (any, error)
}

// Func wraps a function as an Expression.
type Func func(ctx *EvalContext) (any, error)

// Evaluate implements Expression.
func (f Func) Evaluate(ctx *EvalContext) (any, error) {
	return f(ctx)
}

// String implements fmt.Stringer.
func (f Func) String() string {
	return "Func"
}

// MarshalJSON implements json.Marshaler.
func (f Func) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("expression.Func JSON marshaling is not supported")
}

// UnmarshalJSON implements json.Unmarshaler.
func (f Func) UnmarshalJSON([]byte) error {
	return fmt.Errorf("expression.Func JSON unmarshaling is not supported")
}
