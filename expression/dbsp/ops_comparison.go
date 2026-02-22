package dbsp

import (
	"fmt"
	"reflect"

	"github.com/l7mp/dbsp/expression"
)

// eqExpr implements @eq (equality).
type eqExpr struct {
	left  Expression
	right Expression
}

func (e *eqExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	aVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@eq: left operand: %w", err)
	}

	bVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@eq: right operand: %w", err)
	}

	result := deepEqual(aVal, bVal)
	ctx.Logger().V(8).Info("eval", "op", "@eq", "a", aVal, "b", bVal, "result", result)
	return result, nil
}

func (e *eqExpr) String() string { return fmt.Sprintf("@eq(%v, %v)", e.left, e.right) }

// neqExpr implements @neq (not equal).
type neqExpr struct {
	left  Expression
	right Expression
}

func (e *neqExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	aVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@neq: left operand: %w", err)
	}

	bVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@neq: right operand: %w", err)
	}

	result := !deepEqual(aVal, bVal)
	ctx.Logger().V(8).Info("eval", "op", "@neq", "a", aVal, "b", bVal, "result", result)
	return result, nil
}

func (e *neqExpr) String() string { return fmt.Sprintf("@neq(%v, %v)", e.left, e.right) }

// gtExpr implements @gt (greater than).
type gtExpr struct {
	left  Expression
	right Expression
}

func (e *gtExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	return compareNumeric(ctx, e.left, e.right, "@gt", func(cmp int) bool { return cmp > 0 })
}

func (e *gtExpr) String() string { return fmt.Sprintf("@gt(%v, %v)", e.left, e.right) }

// gteExpr implements @gte (greater than or equal).
type gteExpr struct {
	left  Expression
	right Expression
}

func (e *gteExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	return compareNumeric(ctx, e.left, e.right, "@gte", func(cmp int) bool { return cmp >= 0 })
}

func (e *gteExpr) String() string { return fmt.Sprintf("@gte(%v, %v)", e.left, e.right) }

// ltExpr implements @lt (less than).
type ltExpr struct {
	left  Expression
	right Expression
}

func (e *ltExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	return compareNumeric(ctx, e.left, e.right, "@lt", func(cmp int) bool { return cmp < 0 })
}

func (e *ltExpr) String() string { return fmt.Sprintf("@lt(%v, %v)", e.left, e.right) }

// lteExpr implements @lte (less than or equal).
type lteExpr struct {
	left  Expression
	right Expression
}

func (e *lteExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	return compareNumeric(ctx, e.left, e.right, "@lte", func(cmp int) bool { return cmp <= 0 })
}

func (e *lteExpr) String() string { return fmt.Sprintf("@lte(%v, %v)", e.left, e.right) }

// compareNumeric compares two numeric values.
func compareNumeric(ctx *expression.EvalContext, left, right Expression, opName string, cmpFn func(int) bool) (any, error) {
	aVal, err := left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: left operand: %w", opName, err)
	}

	bVal, err := right.Evaluate(ctx)
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
	registerBinaryOp := func(name string, factory func(left, right Expression) Expression) {
		MustRegister(name, func(args any) (Expression, error) {
			left, right, err := asBinaryExprs(args, name)
			if err != nil {
				return nil, err
			}
			return factory(left, right), nil
		})
	}
	registerBinaryOp("@eq", func(l, r Expression) Expression { return &eqExpr{left: l, right: r} })
	registerBinaryOp("@neq", func(l, r Expression) Expression { return &neqExpr{left: l, right: r} })
	registerBinaryOp("@gt", func(l, r Expression) Expression { return &gtExpr{left: l, right: r} })
	registerBinaryOp("@gte", func(l, r Expression) Expression { return &gteExpr{left: l, right: r} })
	registerBinaryOp("@lt", func(l, r Expression) Expression { return &ltExpr{left: l, right: r} })
	registerBinaryOp("@lte", func(l, r Expression) Expression { return &lteExpr{left: l, right: r} })
}
