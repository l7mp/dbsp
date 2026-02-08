package operator

import (
	"fmt"

	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/zset"
)

// Negate returns -Z.
type Negate struct{}

// NewNegate creates a new Negate operator.
func NewNegate() *Negate { return &Negate{} }

// Name implements Operator.
func (o *Negate) Name() string { return "negate" }

// Arity implements Operator.
func (o *Negate) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Negate) Linearity() Linearity { return Linear }

// Apply implements Operator.
func (o *Negate) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	return inputs[0].Negate(), nil
}

// Plus returns A + B.
type Plus struct{}

// NewPlus creates a new Plus operator.
func NewPlus() *Plus { return &Plus{} }

// Name implements Operator.
func (o *Plus) Name() string { return "plus" }

// Arity implements Operator.
func (o *Plus) Arity() int { return 2 }

// Linearity implements Operator.
func (o *Plus) Linearity() Linearity { return Linear }

// Apply implements Operator.
func (o *Plus) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	return inputs[0].Add(inputs[1]), nil
}

// Select filters by predicate.
type Select struct {
	name      string
	predicate expr.Expression
}

// NewSelect creates a new Select operator.
func NewSelect(name string, predicate expr.Expression) *Select {
	return &Select{name: name, predicate: predicate}
}

// Name implements Operator.
func (o *Select) Name() string { return o.name }

// Arity implements Operator.
func (o *Select) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Select) Linearity() Linearity { return Linear }

// Apply implements Operator.
func (o *Select) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	var evalErr error

	inputs[0].Iter(func(elem zset.Document, weight zset.Weight) bool {
		val, err := o.predicate.Evaluate(elem)
		if err != nil {
			evalErr = err
			return false
		}
		if matches, ok := val.(bool); ok && matches {
			result.Insert(elem, weight)
		}
		return true
	})

	if evalErr != nil {
		return zset.ZSet{}, evalErr
	}
	return result, nil
}

// Project transforms elements.
type Project struct {
	name       string
	projection expr.Expression // Must return zset.Element.
}

// NewProject creates a new Project operator.
func NewProject(name string, projection expr.Expression) *Project {
	return &Project{name: name, projection: projection}
}

// Name implements Operator.
func (o *Project) Name() string { return o.name }

// Arity implements Operator.
func (o *Project) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Project) Linearity() Linearity { return Linear }

// Apply implements Operator.
func (o *Project) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	var evalErr error

	inputs[0].Iter(func(elem zset.Document, weight zset.Weight) bool {
		val, err := o.projection.Evaluate(elem)
		if err != nil {
			evalErr = err
			return false
		}
		if val == nil {
			return true
		}
		newElem, ok := val.(zset.Document)
		if !ok {
			evalErr = fmt.Errorf("projection must return zset.Element, got %T", val)
			return false
		}
		result.Insert(newElem, weight)
		return true
	})

	if evalErr != nil {
		return zset.ZSet{}, evalErr
	}
	return result, nil
}
