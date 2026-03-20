package operator

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/datamodel"
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
func (o *LinearCombination) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
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

// Negate returns -Z.
type Negate struct{ linearOp }

// NewNegate creates a new Negate operator.
func NewNegate(opts ...Option) *Negate {
	return &Negate{newLinearOp(KindNegate, 1, "-", opts)}
}

// Apply implements Operator.
func (o *Negate) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
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
func (o *Select) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	var evalErr error

	inputs[0].Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		if o.weightFn != nil && !o.weightFn(weight) {
			return true
		}
		val, err := o.predicate.Evaluate(expression.NewContext(elem.Copy()))
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
	projection expression.Expression // Must return datamodel.Document.
}

// NewProject creates a new Project operator.
func NewProject(projection expression.Expression, opts ...Option) *Project {
	return &Project{
		linearOp:   newLinearOp(KindProject, 1, fmt.Sprintf("π(%s)", projection), opts),
		projection: projection,
	}
}

// Apply implements Operator.
func (o *Project) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	var evalErr error

	inputs[0].Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		val, err := o.projection.Evaluate(expression.NewContext(elem.Copy()))
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
			evalErr = fmt.Errorf("projection must return datamodel.Document, got %T", val)
			return false
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

// Unwind flattens an array field into multiple documents.
// For each element in the array, it produces a copy of the document
// with the array field replaced by that single element.
//
// Example:
//
//	Input:  {id: "x", tags: ["a", "b"]}
//	Output: {id: "x", tags: "a"}, {id: "x", tags: "b"}
type Unwind struct {
	linearOp
	fieldPath  string // The array field to unwind.
	indexField string // Optional: field to store the array index.
	nameAppend bool   // Optional: append index to .metadata.name.
}

// NewUnwind creates a new Unwind operator.
// fieldPath is the name of the array field to unwind.
func NewUnwind(fieldPath string, opts ...Option) *Unwind {
	return &Unwind{
		linearOp:  newLinearOp(KindUnwind, 1, "", opts),
		fieldPath: fieldPath,
	}
}

// WithIndexField sets an optional field to store the array index.
func (o *Unwind) WithIndexField(field string) *Unwind {
	o.indexField = field
	return o
}

// WithNameAppend toggles appending -<index> suffix to .metadata.name.
func (o *Unwind) WithNameAppend(enabled bool) *Unwind {
	o.nameAppend = enabled
	return o
}

// String overrides opBase.String because indexField may be set after construction.
func (o *Unwind) String() string {
	if o.indexField != "" && o.nameAppend {
		return fmt.Sprintf("Unwind(field=%s, index=%s, appendName=true)", o.fieldPath, o.indexField)
	}
	if o.indexField != "" {
		return fmt.Sprintf("Unwind(field=%s, index=%s, appendName=false)", o.fieldPath, o.indexField)
	}
	if o.nameAppend {
		return fmt.Sprintf("Unwind(field=%s, appendName=true)", o.fieldPath)
	}
	return fmt.Sprintf("Unwind(field=%s)", o.fieldPath)
}

// Apply implements Operator.
func (o *Unwind) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
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
		for i, arrElem := range array {
			newDoc := doc.Copy()
			if e := newDoc.SetField(o.fieldPath, arrElem); e != nil {
				err = fmt.Errorf("failed to set field %q: %w", o.fieldPath, e)
				return false
			}

			if o.nameAppend {
				nameRaw, e := newDoc.GetField("metadata.name")
				if e != nil {
					err = fmt.Errorf("failed to read metadata.name for name append: %w", e)
					return false
				}
				name, ok := nameRaw.(string)
				if !ok {
					err = fmt.Errorf("metadata.name must be string for name append, got %T", nameRaw)
					return false
				}
				if e := newDoc.SetField("metadata.name", fmt.Sprintf("%s-%d", name, i)); e != nil {
					err = fmt.Errorf("failed to append index to metadata.name: %w", e)
					return false
				}
			}

			// Optionally set the index field.
			if o.indexField != "" {
				if e := newDoc.SetField(o.indexField, int64(i)); e != nil {
					err = fmt.Errorf("failed to set index field %q: %w", o.indexField, e)
					return false
				}
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
