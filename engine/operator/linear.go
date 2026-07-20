package operator

import (
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/zset"
)

// LinearCombination returns Σ coeffs[i] · inputs[i].
// It is the most general n-ary linear operator: subtraction is coeffs=[+1,-1],
// addition is coeffs=[+1,+1], and arbitrary integer multiples are supported.
type LinearCombination struct {
	linearOp
	coeffs []int
}

// NewLinearCombination creates a new LinearCombination operator. coeffs must
// not be empty; each element is the integer multiplier for the corresponding
// input port.
func NewLinearCombination(coeffs []int, opts ...Option) *LinearCombination {
	c := append([]int(nil), coeffs...)
	return &LinearCombination{
		linearOp: newLinearOp(KindLinearCombination, len(c), fmt.Sprintf("LC(%v)", c), opts),
		coeffs:   c,
	}
}

// Apply implements Operator.
func (o *LinearCombination) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	for i, z := range inputs {
		c := o.coeffs[i]
		switch {
		case c == 1:
			result = result.Add(z)
		case c == -1:
			result = result.Add(z.Negate())
		case c != 0:
			result = result.Add(z.Scale(zset.Weight(c)))
		}
	}
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

// NoOp returns its input unchanged.
type NoOp struct{ linearOp }

// NewNoOp creates a new NoOp operator.
func NewNoOp(opts ...Option) *NoOp {
	return &NoOp{newLinearOp(KindNoOp, 1, "NoOp", opts)}
}

// Apply implements Operator.
func (o *NoOp) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	return inputs[0], nil
}

// Negate returns -Z.
type Negate struct{ linearOp }

// NewNegate creates a new Negate operator.
func NewNegate(opts ...Option) *Negate {
	return &Negate{newLinearOp(KindNegate, 1, "-", opts)}
}

// Apply implements Operator.
func (o *Negate) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	result := inputs[0].Negate()
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

// NewPlus creates a binary addition operator (coefficients [+1, +1]).
func NewPlus(opts ...Option) *LinearCombination {
	return NewLinearCombination([]int{1, 1}, opts...)
}

// NewMinus creates a binary subtraction operator (coefficients [+1, -1]).
func NewMinus(opts ...Option) *LinearCombination {
	return NewLinearCombination([]int{1, -1}, opts...)
}

// NewSum is a backward-compatible alias for NewPlus.
func NewSum(opts ...Option) *LinearCombination { return NewPlus(opts...) }

// NewSubtract is a backward-compatible alias for NewMinus.
func NewSubtract(opts ...Option) *LinearCombination { return NewMinus(opts...) }

// Select filters by predicate.
type Select struct {
	linearOp
	predicate expression.Expression
	weightFn  func(weight zset.Weight) bool
}

// NewSelect creates a new Select operator.
func NewSelect(predicate expression.Expression, opts ...Option) *Select {
	return &Select{
		linearOp:  newLinearOp(KindSelect, 1, fmt.Sprintf("σ(%s)", predicate), opts),
		predicate: predicate,
	}
}

// Apply implements Operator.
func (o *Select) Apply(ctx *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	var evalErr error
	now := execNow(ctx)

	inputs[0].Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		if o.weightFn != nil && !o.weightFn(weight) {
			return true
		}
		val, err := o.predicate.Evaluate(expression.NewContext(elem.Copy()).WithNow(now))
		if err != nil {
			evalErr = err
			return false
		}
		o.logger.V(2).Info("predicate", "expr", o.predicate.String(), "elem", elem.String(), "result", val)
		if matches, ok := val.(bool); ok && matches {
			result.Insert(elem, weight)
		}
		return true
	})

	if evalErr != nil {
		return zset.ZSet{}, evalErr
	}
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

// Project transforms elements.
type Project struct {
	linearOp
	projection expression.Expression
}

// NewProject creates a new Project operator.
func NewProject(projection expression.Expression, opts ...Option) *Project {
	return &Project{
		linearOp:   newLinearOp(KindProject, 1, fmt.Sprintf("π(%s)", projection), opts),
		projection: projection,
	}
}

// Apply implements Operator.
func (o *Project) Apply(ctx *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	var evalErr error
	now := execNow(ctx)

	inputs[0].Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		val, err := o.projection.Evaluate(expression.NewContext(elem.Copy()).WithNow(now))
		if err != nil {
			evalErr = err
			return false
		}
		o.logger.V(2).Info("projection", "expr", o.projection.String(), "elem", elem.String(), "result", val)
		if val == nil {
			return true
		}
		newElem, ok := val.(datamodel.Document)
		if !ok {
			if m, ok := val.(map[string]any); ok {
				u := unstructured.New(map[string]any{})
				for k, v := range m {
					if err := u.SetField(canonicalPath(k), v); err != nil {
						evalErr = fmt.Errorf("projection map field %s: %w", k, err)
						return false
					}
				}
				newElem = u
			} else {
				evalErr = fmt.Errorf("projection must return datamodel.Document, got %T", val)
				return false
			}
		}
		result.Insert(newElem, weight)
		return true
	})

	if evalErr != nil {
		return zset.ZSet{}, evalErr
	}
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

// canonicalPath normalizes a Go-API field-name argument to the $-rooted
// JSONPath the document interface accepts: a bare name F means "$.F" (dots
// traverse, as they always did for these arguments).
func canonicalPath(path string) string {
	if path == "" || strings.HasPrefix(path, "$") {
		return path
	}
	return "$." + path
}

// Unwind expands an array field into multiple documents: for a document
// carrying the list [x, y] at the unwound field, the output contains one
// copy of the document per element, with the field replaced by that
// element.
//
// Example:
//
//	Input:  {id: "x", tags: ["a", "b"]}
//	Output: {id: "x", tags: "a"}, {id: "x", tags: "b"}
//
// The operator is linear and injects nothing beyond the element itself:
// when distinct inputs (or duplicate elements) produce equal output
// documents, their weights merge, per Z-set semantics. Callers that need
// row-distinct outputs or the original element order pair the list with
// @enumerate before unwinding and project an explicit identity downstream.
type Unwind struct {
	linearOp
	fieldPath string // The array field to unwind.
}

// NewUnwind creates a new Unwind operator.
// fieldPath is the array field to unwind; as a Go-API convenience a bare
// dotted field name F is canonicalized to the JSONPath "$.F".
func NewUnwind(fieldPath string, opts ...Option) *Unwind {
	return &Unwind{
		linearOp:  newLinearOp(KindUnwind, 1, "", opts),
		fieldPath: canonicalPath(fieldPath),
	}
}

// String overrides opBase.String to name the unwound field.
func (o *Unwind) String() string {
	return fmt.Sprintf("Unwind(field=%s)", o.fieldPath)
}

// Apply implements Operator.
func (o *Unwind) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()

	var err error
	inputs[0].Iter(func(doc datamodel.Document, weight zset.Weight) bool {
		// Get the array field.
		arrayVal, e := doc.GetField(o.fieldPath)
		if e != nil {
			// Field not found - skip this document.
			o.logger.V(2).Info("unwind-skip", "elem", doc.String(), "field", o.fieldPath, "error", e)
			return true
		}
		if arrayVal == nil {
			return true
		}

		// Convert to []any.
		array, ok := arrayVal.([]any)
		if !ok {
			err = fmt.Errorf("field %q must be []any, got %T", o.fieldPath, arrayVal)
			return false
		}

		// For each element, create a copy with the field replaced.
		for _, arrElem := range array {
			newDoc := doc.Copy()
			if e := newDoc.SetField(o.fieldPath, arrElem); e != nil {
				err = fmt.Errorf("failed to set field %q: %w", o.fieldPath, e)
				return false
			}

			result.Insert(newDoc, weight)
		}
		return true
	})

	if err != nil {
		return zset.ZSet{}, err
	}
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}
