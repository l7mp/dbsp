package operator

import (
	"fmt"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/expression"
	"github.com/l7mp/dbsp/internal/logger"
)

// Negate returns -Z.
type Negate struct {
	jsonUnsupported
	logger logr.Logger
}

// NewNegate creates a new Negate operator.
func NewNegate(opts ...Option) *Negate {
	o := &Negate{}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// String implements fmt.Stringer.
func (o *Negate) String() string { return "-" }

// Arity implements Operator.
func (o *Negate) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Negate) Linearity() Linearity { return Linear }
func (o *Negate) Kind() Kind           { return KindNegate }

// Apply implements Operator.
func (o *Negate) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := inputs[0].Negate()
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

func (o *Negate) Set(_ zset.ZSet)         {}
func (o *Negate) setLogger(l logr.Logger) { o.logger = l }

// LinearCombination returns Σ coeffs[i] · inputs[i].
// It is the most general n-ary linear operator: subtraction is coeffs=[+1,-1],
// addition is coeffs=[+1,+1], and arbitrary integer multiples are supported.
type LinearCombination struct {
	jsonUnsupported
	coeffs []int
	logger logr.Logger
}

// NewLinearCombination creates a new LinearCombination operator. coeffs must
// not be empty; each element is the integer multiplier for the corresponding
// input port.
func NewLinearCombination(coeffs []int, opts ...Option) *LinearCombination {
	o := &LinearCombination{coeffs: coeffs}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// String implements fmt.Stringer.
func (o *LinearCombination) String() string {
	return fmt.Sprintf("LC(%v)", o.coeffs)
}

// Arity implements Operator.
func (o *LinearCombination) Arity() int { return len(o.coeffs) }

// Linearity implements Operator.
func (o *LinearCombination) Linearity() Linearity { return Linear }
func (o *LinearCombination) Kind() Kind           { return KindLinearCombination }

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

func (o *LinearCombination) Set(_ zset.ZSet)         {}
func (o *LinearCombination) setLogger(l logr.Logger) { o.logger = l }

// NewPlus creates a binary addition operator (coefficients [+1, +1]).
func NewPlus(opts ...Option) *LinearCombination {
	return NewLinearCombination([]int{1, 1}, opts...)
}

// NewMinus creates a binary subtraction operator (coefficients [+1, -1]).
func NewMinus(opts ...Option) *LinearCombination {
	return NewLinearCombination([]int{1, -1}, opts...)
}

// NewSum is a backward-compatible alias for NewPlus.
func NewSum(opts ...Option) *LinearCombination {
	return NewPlus(opts...)
}

// NewSubtract is a backward-compatible alias for NewMinus.
func NewSubtract(opts ...Option) *LinearCombination {
	return NewMinus(opts...)
}

// Select filters by predicate.
type Select struct {
	jsonUnsupported
	predicate expression.Expression
	weightFn  func(weight zset.Weight) bool
	logger    logr.Logger
}

// NewSelect creates a new Select operator.
func NewSelect(predicate expression.Expression, opts ...Option) *Select {
	o := &Select{predicate: predicate}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// String implements fmt.Stringer.
func (o *Select) String() string {
	return fmt.Sprintf("σ(%s)", o.predicate)
}

// Arity implements Operator.
func (o *Select) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Select) Linearity() Linearity { return Linear }
func (o *Select) Kind() Kind           { return KindSelect }

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

func (o *Select) Set(_ zset.ZSet)         {}
func (o *Select) setLogger(l logr.Logger) { o.logger = l }

// Project transforms elements.
type Project struct {
	jsonUnsupported
	projection expression.Expression // Must return datamodel.Document.
	logger     logr.Logger
}

// NewProject creates a new Project operator.
func NewProject(projection expression.Expression, opts ...Option) *Project {
	o := &Project{projection: projection}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// String implements fmt.Stringer.
func (o *Project) String() string {
	return fmt.Sprintf("π(%s)", o.projection)
}

// Arity implements Operator.
func (o *Project) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Project) Linearity() Linearity { return Linear }
func (o *Project) Kind() Kind           { return KindProject }

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

func (o *Project) Set(_ zset.ZSet)         {}
func (o *Project) setLogger(l logr.Logger) { o.logger = l }

// Unwind flattens an array field into multiple documents.
// For each element in the array, it produces a copy of the document
// with the array field replaced by that single element.
//
// Example:
//
//	Input:  {id: "x", tags: ["a", "b"]}
//	Output: {id: "x", tags: "a"}, {id: "x", tags: "b"}
type Unwind struct {
	jsonUnsupported
	fieldPath  string // The array field to unwind.
	indexField string // Optional: field to store the array index.
	logger     logr.Logger
}

// NewUnwind creates a new Unwind operator.
// fieldPath is the name of the array field to unwind.
func NewUnwind(fieldPath string, opts ...Option) *Unwind {
	o := &Unwind{fieldPath: fieldPath}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// WithIndexField sets an optional field to store the array index.
func (o *Unwind) WithIndexField(field string) *Unwind {
	o.indexField = field
	return o
}

// String implements fmt.Stringer.
func (o *Unwind) String() string {
	if o.indexField != "" {
		return fmt.Sprintf("Unwind(field=%s, index=%s)", o.fieldPath, o.indexField)
	}
	return fmt.Sprintf("Unwind(field=%s)", o.fieldPath)
}

// Arity implements Operator.
func (o *Unwind) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Unwind) Linearity() Linearity { return Linear }
func (o *Unwind) Kind() Kind           { return KindUnwind }

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

func (o *Unwind) Set(_ zset.ZSet)         {}
func (o *Unwind) setLogger(l logr.Logger) { o.logger = l }
