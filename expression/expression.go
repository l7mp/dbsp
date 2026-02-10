// Package expr provides a minimal expression interface for DBSP operators.
package expression

import (
	"fmt"

	"github.com/l7mp/dbsp/datamodel"
)

// Expression is the minimal interface for data access and transformation.
type Expression interface {
	fmt.Stringer

	Evaluate(doc datamodel.Document) (any, error)
}

// Func wraps a function as an Expression.
type Func func(elem datamodel.Document) (any, error)

// Evaluate implements Expression.
func (f Func) Evaluate(elem datamodel.Document) (any, error) {
	return f(elem)
}

// String implements fmt.Stringer.
func (f Func) String() string {
	return "Func"
}
