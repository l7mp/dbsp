package operator

import (
	"fmt"

	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/zset"
)

// UnwindInput is passed to the output expression.
type UnwindInput struct {
	elem    zset.Document
	element any
	index   int
}

// Key implements zset.Document (returns empty, used for expression evaluation).
func (u UnwindInput) Key() string { return "" }

// PrimaryKey implements zset.Document (returns empty, used for expression evaluation).
func (u UnwindInput) PrimaryKey() (string, error) { return "", nil }

// Elem returns the original element.
func (u UnwindInput) Elem() zset.Document { return u.elem }

// Element returns the array element being unwound.
func (u UnwindInput) Element() any { return u.element }

// Index returns the array index of the unwound element.
func (u UnwindInput) Index() int { return u.index }

// Unwind flattens arrays.
type Unwind struct {
	name       string
	pathExpr   expr.Expression // Element -> []any.
	outputExpr expr.Expression // UnwindInput -> Element.
}

// NewUnwind creates a new Unwind operator.
func NewUnwind(name string, pathExpr, outputExpr expr.Expression) *Unwind {
	return &Unwind{name: name, pathExpr: pathExpr, outputExpr: outputExpr}
}

// Name implements Operator.
func (o *Unwind) Name() string { return o.name }

// Arity implements Operator.
func (o *Unwind) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Unwind) Linearity() Linearity { return Linear }

// Apply implements Operator.
func (o *Unwind) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	var err error

	inputs[0].Iter(func(elem zset.Document, weight zset.Weight) bool {
		arrayVal, e := o.pathExpr.Evaluate(elem)
		if e != nil {
			err = e
			return false
		}
		if arrayVal == nil {
			return true
		}

		array, ok := arrayVal.([]any)
		if !ok {
			err = fmt.Errorf("path must return []any, got %T", arrayVal)
			return false
		}

		for i, arrElem := range array {
			outVal, e := o.outputExpr.Evaluate(UnwindInput{elem: elem, element: arrElem, index: i})
			if e != nil {
				err = e
				return false
			}
			if outElem, ok := outVal.(zset.Document); ok {
				result.Insert(outElem, weight)
			}
		}
		return true
	})

	if err != nil {
		return zset.ZSet{}, err
	}
	return result, nil
}
