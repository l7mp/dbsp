package dbsp

import (
	"encoding/json"
	"fmt"

	"github.com/l7mp/dbsp/engine/expression"
)

// mapExpr implements @map - iterates over a list, evaluating an expression for each element.
type mapExpr struct{ binaryOp }

func (e *mapExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	listValue, err := e.right.Evaluate(ctx)
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
		v, err := e.left.Evaluate(itemCtx)
		if err != nil {
			return nil, fmt.Errorf("@map[%d]: %w", i, err)
		}
		result[i] = v
	}

	ctx.Logger().V(8).Info("eval", "op", "@map", "result", result)
	return result, nil
}

// filterExpr implements @filter - filters a list based on a predicate.
type filterExpr struct{ binaryOp }

func (e *filterExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	listValue, err := e.right.Evaluate(ctx)
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
		v, err := e.left.Evaluate(itemCtx)
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

// sumExpr implements @sum - sums all elements.
type sumExpr struct{ variadicOp }

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

// lenExpr implements @len - returns the length of a list or string.
type lenExpr struct{ unaryOp }

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

// minExpr implements @min - returns the minimum value in a list.
type minExpr struct{ variadicOp }

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

// lexMinExpr implements @lexmin - returns the lexicographically minimal element.
type lexMinExpr struct{ variadicOp }

func (e *lexMinExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	values, err := evaluateLexArgs(ctx, e.args, "@lexmin")
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}

	best := values[0]
	bestKey, err := lexKey(best)
	if err != nil {
		return nil, fmt.Errorf("@lexmin[0]: %w", err)
	}

	for i := 1; i < len(values); i++ {
		k, err := lexKey(values[i])
		if err != nil {
			return nil, fmt.Errorf("@lexmin[%d]: %w", i, err)
		}
		if k < bestKey {
			best = values[i]
			bestKey = k
		}
	}

	ctx.Logger().V(8).Info("eval", "op", "@lexmin", "result", best)
	return best, nil
}

// maxExpr implements @max - returns the maximum value in a list.
type maxExpr struct{ variadicOp }

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

// lexMaxExpr implements @lexmax - returns the lexicographically maximal element.
type lexMaxExpr struct{ variadicOp }

func (e *lexMaxExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	values, err := evaluateLexArgs(ctx, e.args, "@lexmax")
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}

	best := values[0]
	bestKey, err := lexKey(best)
	if err != nil {
		return nil, fmt.Errorf("@lexmax[0]: %w", err)
	}

	for i := 1; i < len(values); i++ {
		k, err := lexKey(values[i])
		if err != nil {
			return nil, fmt.Errorf("@lexmax[%d]: %w", i, err)
		}
		if k > bestKey {
			best = values[i]
			bestKey = k
		}
	}

	ctx.Logger().V(8).Info("eval", "op", "@lexmax", "result", best)
	return best, nil
}

// inExpr implements @in - checks if an element is in a list.
type inExpr struct{ binaryOp }

func (e *inExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	elemVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@in: element: %w", err)
	}

	listVal, err := e.right.Evaluate(ctx)
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

// rangeExpr implements @range - creates a range of integers [1..n].
type rangeExpr struct{ unaryOp }

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

func evaluateListArgs(ctx *expression.EvalContext, args []Expression, opName string) ([]any, error) {
	result := make([]any, len(args))
	for i, elem := range args {
		v, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", opName, i, err)
		}
		result[i] = v
	}
	return result, nil
}

func evaluateLexArgs(ctx *expression.EvalContext, args []Expression, opName string) ([]any, error) {
	values, err := evaluateListArgs(ctx, args, opName)
	if err != nil {
		return nil, err
	}
	if len(values) == 1 {
		if list, err := AsList(values[0]); err == nil {
			return list, nil
		}
	}
	return values, nil
}

func lexKey(v any) (string, error) {
	type hasher interface{ Hash() string }
	if h, ok := v.(hasher); ok {
		return h.Hash(), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("cannot serialize value: %w", err)
	}
	return string(b), nil
}

func init() {
	MustRegister("@map", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || len(list) != 2 {
			return nil, fmt.Errorf("@map: expected [expression, list] arguments")
		}
		return &mapExpr{binaryOp{"@map", list[0], list[1]}}, nil
	})
	MustRegister("@filter", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || len(list) != 2 {
			return nil, fmt.Errorf("@filter: expected [predicate, list] arguments")
		}
		return &filterExpr{binaryOp{"@filter", list[0], list[1]}}, nil
	})
	MustRegister("@sum", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@sum: %w", err)
		}
		return &sumExpr{variadicOp{"@sum", list}}, nil
	})
	MustRegister("@len", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@len: %w", err)
		}
		return &lenExpr{unaryOp{"@len", operand}}, nil
	})
	MustRegister("@min", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@min: %w", err)
		}
		return &minExpr{variadicOp{"@min", list}}, nil
	})
	MustRegister("@max", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@max: %w", err)
		}
		return &maxExpr{variadicOp{"@max", list}}, nil
	})
	MustRegister("@lexmin", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@lexmin: %w", err)
		}
		return &lexMinExpr{variadicOp{"@lexmin", list}}, nil
	})
	MustRegister("@lexmax", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@lexmax: %w", err)
		}
		return &lexMaxExpr{variadicOp{"@lexmax", list}}, nil
	})
	MustRegister("@in", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || len(list) != 2 {
			return nil, fmt.Errorf("@in: expected [element, list] arguments")
		}
		return &inExpr{binaryOp{"@in", list[0], list[1]}}, nil
	})
	MustRegister("@range", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@range: %w", err)
		}
		return &rangeExpr{unaryOp{"@range", operand}}, nil
	})
}
