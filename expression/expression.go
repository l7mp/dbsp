// Package expr provides a minimal expression interface for DBSP operators.
package expression

import "fmt"

// Expression is the minimal interface for data access and transformation.
type Expression interface {
	fmt.Stringer

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
