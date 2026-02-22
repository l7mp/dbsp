package dbsp

import (
	"fmt"

	"github.com/l7mp/dbsp/expression"
)

// mapExpr implements @map - iterates over a list, evaluating an expression for each element.
type mapExpr struct {
	mapFn Expression
	list  Expression
}

func (e *mapExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	listValue, err := e.list.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@map: failed to evaluate list: %w", err)
	}

	list, err := AsList(listValue)
	if err != nil {
		return nil, fmt.Errorf("@map: second argument must be a list: %w", err)
	}

	result := make([]any, len(list))
	for i, item := range list {
		itemCtx := ctx.WithSubject(item)
		v, err := e.mapFn.Evaluate(itemCtx)
		if err != nil {
			return nil, fmt.Errorf("@map[%d]: %w", i, err)
		}
		result[i] = v
	}

	ctx.Logger().V(8).Info("eval", "op", "@map", "result", result)
	return result, nil
}

func (e *mapExpr) String() string { return fmt.Sprintf("@map(%v, %v)", e.mapFn, e.list) }

// filterExpr implements @filter - filters a list based on a predicate.
type filterExpr struct {
	predicate Expression
	list      Expression
}

func (e *filterExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	listValue, err := e.list.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@filter: failed to evaluate list: %w", err)
	}

	list, err := AsList(listValue)
	if err != nil {
		return nil, fmt.Errorf("@filter: second argument must be a list: %w", err)
	}

	result := make([]any, 0, len(list))
	for i, item := range list {
		itemCtx := ctx.WithSubject(item)
		v, err := e.predicate.Evaluate(itemCtx)
		if err != nil {
			return nil, fmt.Errorf("@filter[%d]: %w", i, err)
		}

		keep, err := AsBool(v)
		if err != nil {
			return nil, fmt.Errorf("@filter[%d]: predicate must return bool: %w", i, err)
		}

		if keep {
			result = append(result, item)
		}
	}

	ctx.Logger().V(8).Info("eval", "op", "@filter", "result", result)
	return result, nil
}

func (e *filterExpr) String() string {
	return fmt.Sprintf("@filter(%v, %v)", e.predicate, e.list)
}

// sumExpr implements @sum - sums all elements.
type sumExpr struct {
	args []Expression
}

func (e *sumExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	// Evaluate all args.
	values := make([]any, len(e.args))
	for i, elem := range e.args {
		v, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@sum[%d]: %w", i, err)
		}
		values[i] = v
	}

	if len(values) == 0 {
		return int64(0), nil
	}

	isFloat := false
	for _, item := range values {
		if IsFloat(item) {
			isFloat = true
			break
		}
	}

	if isFloat {
		var sum float64
		for i, item := range values {
			f, err := AsFloat(item)
			if err != nil {
				return nil, fmt.Errorf("@sum[%d]: %w", i, err)
			}
			sum += f
		}
		ctx.Logger().V(8).Info("eval", "op", "@sum", "result", sum)
		return sum, nil
	}

	var sum int64
	for i, item := range values {
		n, err := AsInt(item)
		if err != nil {
			return nil, fmt.Errorf("@sum[%d]: %w", i, err)
		}
		sum += n
	}
	ctx.Logger().V(8).Info("eval", "op", "@sum", "result", sum)
	return sum, nil
}

func (e *sumExpr) String() string { return fmt.Sprintf("@sum(%v)", e.args) }

// lenExpr implements @len - returns the length of a list or string.
type lenExpr struct {
	operand Expression
}

func (e *lenExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	switch v := value.(type) {
	case string:
		result := int64(len(v))
		ctx.Logger().V(8).Info("eval", "op", "@len", "result", result)
		return result, nil
	case []any:
		result := int64(len(v))
		ctx.Logger().V(8).Info("eval", "op", "@len", "result", result)
		return result, nil
	case map[string]any:
		result := int64(len(v))
		ctx.Logger().V(8).Info("eval", "op", "@len", "result", result)
		return result, nil
	default:
		list, err := AsList(value)
		if err != nil {
			return nil, fmt.Errorf("@len: cannot get length of %T", value)
		}
		result := int64(len(list))
		ctx.Logger().V(8).Info("eval", "op", "@len", "result", result)
		return result, nil
	}
}

func (e *lenExpr) String() string { return fmt.Sprintf("@len(%v)", e.operand) }

// minExpr implements @min - returns the minimum value in a list.
type minExpr struct {
	args []Expression
}

func (e *minExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	list, err := evaluateNumericList(ctx, e.args, "@min")
	if err != nil {
		return nil, err
	}

	if len(list) == 0 {
		return nil, fmt.Errorf("@min: empty list")
	}

	allInt := true
	for _, item := range list {
		if !IsInt(item) {
			allInt = false
			break
		}
	}

	if allInt {
		minVal, _ := AsInt(list[0])
		for i := 1; i < len(list); i++ {
			v, _ := AsInt(list[i])
			if v < minVal {
				minVal = v
			}
		}
		ctx.Logger().V(8).Info("eval", "op", "@min", "result", minVal)
		return minVal, nil
	}

	minVal, _ := AsFloat(list[0])
	for i := 1; i < len(list); i++ {
		v, _ := AsFloat(list[i])
		if v < minVal {
			minVal = v
		}
	}
	ctx.Logger().V(8).Info("eval", "op", "@min", "result", minVal)
	return minVal, nil
}

func (e *minExpr) String() string { return fmt.Sprintf("@min(%v)", e.args) }

// maxExpr implements @max - returns the maximum value in a list.
type maxExpr struct {
	args []Expression
}

func (e *maxExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	list, err := evaluateNumericList(ctx, e.args, "@max")
	if err != nil {
		return nil, err
	}

	if len(list) == 0 {
		return nil, fmt.Errorf("@max: empty list")
	}

	allInt := true
	for _, item := range list {
		if !IsInt(item) {
			allInt = false
			break
		}
	}

	if allInt {
		maxVal, _ := AsInt(list[0])
		for i := 1; i < len(list); i++ {
			v, _ := AsInt(list[i])
			if v > maxVal {
				maxVal = v
			}
		}
		ctx.Logger().V(8).Info("eval", "op", "@max", "result", maxVal)
		return maxVal, nil
	}

	maxVal, _ := AsFloat(list[0])
	for i := 1; i < len(list); i++ {
		v, _ := AsFloat(list[i])
		if v > maxVal {
			maxVal = v
		}
	}
	ctx.Logger().V(8).Info("eval", "op", "@max", "result", maxVal)
	return maxVal, nil
}

func (e *maxExpr) String() string { return fmt.Sprintf("@max(%v)", e.args) }

// inExpr implements @in - checks if an element is in a list.
type inExpr struct {
	element Expression
	list    Expression
}

func (e *inExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	elemVal, err := e.element.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@in: element: %w", err)
	}

	listVal, err := e.list.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@in: list: %w", err)
	}

	list, err := AsList(listVal)
	if err != nil {
		return nil, fmt.Errorf("@in: second argument must be a list: %w", err)
	}

	for _, item := range list {
		if deepEqual(elemVal, item) {
			ctx.Logger().V(8).Info("eval", "op", "@in", "result", true)
			return true, nil
		}
	}

	ctx.Logger().V(8).Info("eval", "op", "@in", "result", false)
	return false, nil
}

func (e *inExpr) String() string { return fmt.Sprintf("@in(%v, %v)", e.element, e.list) }

// rangeExpr implements @range - creates a range of integers [1..n].
type rangeExpr struct {
	operand Expression
}

func (e *rangeExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	n, err := AsInt(value)
	if err != nil {
		return nil, fmt.Errorf("@range: %w", err)
	}

	if n < 0 {
		return nil, fmt.Errorf("@range: argument must be non-negative")
	}

	result := make([]any, n)
	for i := int64(0); i < n; i++ {
		result[i] = i + 1
	}

	ctx.Logger().V(8).Info("eval", "op", "@range", "n", n, "result", result)
	return result, nil
}

func (e *rangeExpr) String() string { return fmt.Sprintf("@range(%v)", e.operand) }

// evaluateNumericList evaluates a list of expression args and validates they are numeric.
func evaluateNumericList(ctx *expression.EvalContext, args []Expression, opName string) ([]any, error) {
	result := make([]any, len(args))
	for i, elem := range args {
		v, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", opName, i, err)
		}
		if !IsNumeric(v) {
			return nil, fmt.Errorf("%s[%d]: expected numeric, got %T", opName, i, v)
		}
		result[i] = v
	}
	return result, nil
}

func init() {
	MustRegister("@map", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || len(list) != 2 {
			return nil, fmt.Errorf("@map: expected [expression, list] arguments")
		}
		return &mapExpr{mapFn: list[0], list: list[1]}, nil
	})
	MustRegister("@filter", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || len(list) != 2 {
			return nil, fmt.Errorf("@filter: expected [predicate, list] arguments")
		}
		return &filterExpr{predicate: list[0], list: list[1]}, nil
	})
	MustRegister("@sum", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@sum: %w", err)
		}
		return &sumExpr{args: list}, nil
	})
	MustRegister("@len", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@len: %w", err)
		}
		return &lenExpr{operand: operand}, nil
	})
	MustRegister("@min", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@min: %w", err)
		}
		return &minExpr{args: list}, nil
	})
	MustRegister("@max", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@max: %w", err)
		}
		return &maxExpr{args: list}, nil
	})
	MustRegister("@in", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || len(list) != 2 {
			return nil, fmt.Errorf("@in: expected [element, list] arguments")
		}
		return &inExpr{element: list[0], list: list[1]}, nil
	})
	MustRegister("@range", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@range: %w", err)
		}
		return &rangeExpr{operand: operand}, nil
	})
}
