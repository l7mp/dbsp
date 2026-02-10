package dbsp

import (
	"fmt"
	"reflect"
)

// EqOp implements @eq (equality).
type EqOp struct{}

func (o *EqOp) Name() string { return "@eq" }

func (o *EqOp) Evaluate(ctx *Context, args Args) (any, error) {
	elements, err := getBinaryElements(args, o.Name())
	if err != nil {
		return nil, err
	}

	aVal, err := elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: left operand: %w", o.Name(), err)
	}

	bVal, err := elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: right operand: %w", o.Name(), err)
	}

	result := deepEqual(aVal, bVal)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "a", aVal, "b", bVal, "result", result)
	return result, nil
}

// NeqOp implements @neq (not equal).
type NeqOp struct{}

func (o *NeqOp) Name() string { return "@neq" }

func (o *NeqOp) Evaluate(ctx *Context, args Args) (any, error) {
	elements, err := getBinaryElements(args, o.Name())
	if err != nil {
		return nil, err
	}

	aVal, err := elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: left operand: %w", o.Name(), err)
	}

	bVal, err := elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: right operand: %w", o.Name(), err)
	}

	result := !deepEqual(aVal, bVal)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "a", aVal, "b", bVal, "result", result)
	return result, nil
}

// GtOp implements @gt (greater than).
type GtOp struct{}

func (o *GtOp) Name() string { return "@gt" }

func (o *GtOp) Evaluate(ctx *Context, args Args) (any, error) {
	return compareNumeric(ctx, args, o.Name(), func(cmp int) bool { return cmp > 0 })
}

// GteOp implements @gte (greater than or equal).
type GteOp struct{}

func (o *GteOp) Name() string { return "@gte" }

func (o *GteOp) Evaluate(ctx *Context, args Args) (any, error) {
	return compareNumeric(ctx, args, o.Name(), func(cmp int) bool { return cmp >= 0 })
}

// LtOp implements @lt (less than).
type LtOp struct{}

func (o *LtOp) Name() string { return "@lt" }

func (o *LtOp) Evaluate(ctx *Context, args Args) (any, error) {
	return compareNumeric(ctx, args, o.Name(), func(cmp int) bool { return cmp < 0 })
}

// LteOp implements @lte (less than or equal).
type LteOp struct{}

func (o *LteOp) Name() string { return "@lte" }

func (o *LteOp) Evaluate(ctx *Context, args Args) (any, error) {
	return compareNumeric(ctx, args, o.Name(), func(cmp int) bool { return cmp <= 0 })
}

// compareNumeric compares two numeric values.
func compareNumeric(ctx *Context, args Args, opName string, cmpFn func(int) bool) (any, error) {
	elements, err := getBinaryElements(args, opName)
	if err != nil {
		return nil, err
	}

	aVal, err := elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: left operand: %w", opName, err)
	}

	bVal, err := elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: right operand: %w", opName, err)
	}

	// Handle string comparison.
	if aStr, ok := aVal.(string); ok {
		bStr, ok := bVal.(string)
		if !ok {
			return nil, fmt.Errorf("%s: cannot compare string with %T", opName, bVal)
		}
		var cmp int
		if aStr < bStr {
			cmp = -1
		} else if aStr > bStr {
			cmp = 1
		}
		result := cmpFn(cmp)
		ctx.Logger().V(8).Info("eval", "op", opName, "a", aStr, "b", bStr, "result", result)
		return result, nil
	}

	// Handle numeric comparison.
	numType, err := GetNumericType(aVal, bVal)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", opName, err)
	}

	var cmp int
	if numType == NumericTypeInt {
		a, _ := AsInt(aVal)
		b, _ := AsInt(bVal)
		if a < b {
			cmp = -1
		} else if a > b {
			cmp = 1
		}
	} else {
		a, _ := AsFloat(aVal)
		b, _ := AsFloat(bVal)
		if a < b {
			cmp = -1
		} else if a > b {
			cmp = 1
		}
	}

	result := cmpFn(cmp)
	ctx.Logger().V(8).Info("eval", "op", opName, "a", aVal, "b", bVal, "result", result)
	return result, nil
}

// deepEqual performs deep equality comparison, normalizing numeric types.
func deepEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle numeric type normalization.
	if IsNumeric(a) && IsNumeric(b) {
		// If both are int, compare as int.
		if IsInt(a) && IsInt(b) {
			aInt, _ := AsInt(a)
			bInt, _ := AsInt(b)
			return aInt == bInt
		}
		// Otherwise compare as float.
		aFloat, err1 := AsFloat(a)
		bFloat, err2 := AsFloat(b)
		if err1 == nil && err2 == nil {
			return aFloat == bFloat
		}
	}

	// Fall back to reflect.DeepEqual for other types.
	return reflect.DeepEqual(a, b)
}

func init() {
	MustRegister("@eq", func() Operator { return &EqOp{} })
	MustRegister("@neq", func() Operator { return &NeqOp{} })
	MustRegister("@gt", func() Operator { return &GtOp{} })
	MustRegister("@gte", func() Operator { return &GteOp{} })
	MustRegister("@lt", func() Operator { return &LtOp{} })
	MustRegister("@lte", func() Operator { return &LteOp{} })
}
