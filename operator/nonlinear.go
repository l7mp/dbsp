package operator

import (
	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/zset"
)

// Distinct converts Z-set to set (all positive weights become 1).
type Distinct struct {
	name string
}

// NewDistinct creates a new Distinct operator.
func NewDistinct(name string) *Distinct {
	return &Distinct{name: name}
}

// Name implements Operator.
func (o *Distinct) Name() string { return o.name }

// Arity implements Operator.
func (o *Distinct) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Distinct) Linearity() Linearity { return NonLinear }

// Apply implements Operator.
func (o *Distinct) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	inputs[0].Iter(func(elem zset.Element, weight zset.Weight) bool {
		if weight > 0 {
			result.Insert(elem, 1)
		}
		return true
	})
	return result, nil
}

// FoldInput is passed to the fold expression.
type FoldInput struct {
	acc    any
	elem   zset.Element
	weight zset.Weight
}

// Key implements zset.Element (returns empty, used for expression evaluation).
func (f FoldInput) Key() string { return "" }

// Acc returns the current accumulator value.
func (f FoldInput) Acc() any { return f.acc }

// Elem returns the element being folded.
func (f FoldInput) Elem() zset.Element { return f.elem }

// Weight returns the element's weight.
func (f FoldInput) Weight() zset.Weight { return f.weight }

// GroupOutput is passed to the output expression.
type GroupOutput struct {
	groupKey any
	acc      any
}

// Key implements zset.Element (returns empty, used for expression evaluation).
func (g GroupOutput) Key() string { return "" }

// GroupKey returns the grouping key.
func (g GroupOutput) GroupKey() any { return g.groupKey }

// Acc returns the final accumulator value.
func (g GroupOutput) Acc() any { return g.acc }

// Group performs GROUP BY with aggregation.
type Group struct {
	name       string
	keyExpr    expr.Expression // Element -> key (any).
	zeroExpr   expr.Expression // key -> initial acc.
	foldExpr   expr.Expression // FoldInput -> new acc.
	outputExpr expr.Expression // GroupOutput -> Element.
}

// NewGroup creates a new Group operator.
func NewGroup(name string, keyExpr, zeroExpr, foldExpr, outputExpr expr.Expression) *Group {
	return &Group{
		name:       name,
		keyExpr:    keyExpr,
		zeroExpr:   zeroExpr,
		foldExpr:   foldExpr,
		outputExpr: outputExpr,
	}
}

// Name implements Operator.
func (o *Group) Name() string { return o.name }

// Arity implements Operator.
func (o *Group) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Group) Linearity() Linearity { return NonLinear }

// Apply implements Operator.
func (o *Group) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	groups := make(map[any]any) // key -> accumulator.

	var err error
	inputs[0].Iter(func(elem zset.Element, weight zset.Weight) bool {
		keyVal, e := o.keyExpr.Evaluate(elem)
		if e != nil {
			err = e
			return false
		}

		if _, exists := groups[keyVal]; !exists {
			zero, e := o.zeroExpr.Evaluate(nil)
			if e != nil {
				err = e
				return false
			}
			groups[keyVal] = zero
		}

		foldIn := FoldInput{acc: groups[keyVal], elem: elem, weight: weight}
		newAcc, e := o.foldExpr.Evaluate(foldIn)
		if e != nil {
			err = e
			return false
		}
		groups[keyVal] = newAcc

		return true
	})

	if err != nil {
		return zset.ZSet{}, err
	}

	result := zset.New()
	for key, acc := range groups {
		outVal, e := o.outputExpr.Evaluate(GroupOutput{groupKey: key, acc: acc})
		if e != nil {
			return zset.ZSet{}, e
		}
		if outElem, ok := outVal.(zset.Element); ok {
			result.Insert(outElem, 1)
		}
	}

	return result, nil
}
