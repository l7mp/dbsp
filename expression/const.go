package expression

import (
	"fmt"

	"github.com/l7mp/dbsp/datamodel"
)

// Const is an expression that always returns the same value.
type Const struct {
	Value any
}

// NewConst creates a constant expression.
func NewConst(value any) *Const {
	return &Const{Value: value}
}

// Evaluate implements Expression.
func (c *Const) Evaluate(elem datamodel.Document) (any, error) {
	return c.Value, nil
}

// String implements fmt.Stringer.
func (c *Const) String() string {
	return fmt.Sprintf("Const(%v)", c.Value)
}
