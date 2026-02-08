// Package expr provides a minimal expression interface for DBSP operators.
package expr

import "github.com/l7mp/dbsp/zset"

// Expression is the minimal interface for data access and transformation.
type Expression interface {
	Evaluate(elem zset.Document) (any, error)
}

// Func wraps a function as an Expression.
type Func func(elem zset.Document) (any, error)

// Evaluate implements Expression.
func (f Func) Evaluate(elem zset.Document) (any, error) {
	return f(elem)
}
