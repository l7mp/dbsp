package expression

import "fmt"

// Compiled wraps an evaluator function with an optional serializable source expression.
//
// Evaluate always uses eval. MarshalJSON/String prefer original when present so
// compiled closures can still be embedded in JSON-serializable circuits.
type Compiled struct {
	eval     Func
	original Expression
}

// NewCompiled creates an expression backed by eval and serialized as original.
func NewCompiled(eval Func, original Expression) *Compiled {
	return &Compiled{eval: eval, original: original}
}

// Evaluate implements Expression.
func (c *Compiled) Evaluate(ctx *EvalContext) (any, error) {
	if c == nil || c.eval == nil {
		return nil, fmt.Errorf("compiled expression has no evaluator")
	}
	return c.eval(ctx)
}

// String implements fmt.Stringer.
func (c *Compiled) String() string {
	if c != nil && c.original != nil {
		return c.original.String()
	}
	return "Compiled"
}

// MarshalJSON implements json.Marshaler.
func (c *Compiled) MarshalJSON() ([]byte, error) {
	if c == nil || c.original == nil {
		return nil, fmt.Errorf("compiled expression JSON marshaling requires an original expression")
	}
	return c.original.MarshalJSON()
}

// UnmarshalJSON implements json.Unmarshaler.
func (c *Compiled) UnmarshalJSON([]byte) error {
	return fmt.Errorf("compiled expression JSON unmarshaling is not supported")
}
