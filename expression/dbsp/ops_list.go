package dbsp

import "fmt"

// MapOp implements @map - iterates over a list, evaluating an expression for each element.
// The current element is available as the "subject" in the context.
// Arguments: [mapExpr, listExpr]
type MapOp struct{}

func (o *MapOp) Name() string { return "@map" }

func (o *MapOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@map: expected [expression, list] arguments")
	}

	// First element is the mapping expression (evaluated per item).
	mapExpr := listArgs.Elements[0]

	// Second element is the list to iterate (evaluated once).
	listValue, err := listArgs.Elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@map: failed to evaluate list: %w", err)
	}

	list, err := AsList(listValue)
	if err != nil {
		return nil, fmt.Errorf("@map: second argument must be a list: %w", err)
	}

	// Map over the list, passing each element as the subject.
	result := make([]any, len(list))
	for i, item := range list {
		itemCtx := ctx.WithSubject(item)
		v, err := mapExpr.Eval(itemCtx)
		if err != nil {
			return nil, fmt.Errorf("@map[%d]: %w", i, err)
		}
		result[i] = v
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// FilterOp implements @filter - filters a list based on a predicate.
// Arguments: [predicateExpr, listExpr]
type FilterOp struct{}

func (o *FilterOp) Name() string { return "@filter" }

func (o *FilterOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@filter: expected [predicate, list] arguments")
	}

	predExpr := listArgs.Elements[0]

	listValue, err := listArgs.Elements[1].Eval(ctx)
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
		v, err := predExpr.Eval(itemCtx)
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

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// SumOp implements @sum - sums all elements of a list.
type SumOp struct{}

func (o *SumOp) Name() string { return "@sum" }

func (o *SumOp) Evaluate(ctx *Context, args Args) (any, error) {
	list, err := o.getList(ctx, args)
	if err != nil {
		return nil, err
	}

	if len(list) == 0 {
		return int64(0), nil
	}

	// Determine if result should be int or float.
	isFloat := false
	for _, item := range list {
		if IsFloat(item) {
			isFloat = true
			break
		}
	}

	if isFloat {
		var sum float64
		for i, item := range list {
			f, err := AsFloat(item)
			if err != nil {
				return nil, fmt.Errorf("@sum[%d]: %w", i, err)
			}
			sum += f
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", sum)
		return sum, nil
	}

	var sum int64
	for i, item := range list {
		n, err := AsInt(item)
		if err != nil {
			return nil, fmt.Errorf("@sum[%d]: %w", i, err)
		}
		sum += n
	}
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", sum)
	return sum, nil
}

func (o *SumOp) getList(ctx *Context, args Args) ([]any, error) {
	switch a := args.(type) {
	case ListArgs:
		result := make([]any, len(a.Elements))
		for i, elem := range a.Elements {
			v, err := elem.Eval(ctx)
			if err != nil {
				return nil, fmt.Errorf("@sum[%d]: %w", i, err)
			}
			result[i] = v
		}
		return result, nil
	case UnaryArgs:
		v, err := a.Operand.Eval(ctx)
		if err != nil {
			return nil, err
		}
		return AsList(v)
	case LiteralArgs:
		return AsList(a.Value)
	default:
		return nil, fmt.Errorf("@sum: unexpected args type %T", args)
	}
}

// LenOp implements @len - returns the length of a list or string.
type LenOp struct{}

func (o *LenOp) Name() string { return "@len" }

func (o *LenOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	switch v := value.(type) {
	case string:
		result := int64(len(v))
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
		return result, nil
	case []any:
		result := int64(len(v))
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
		return result, nil
	case map[string]any:
		result := int64(len(v))
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
		return result, nil
	default:
		list, err := AsList(value)
		if err != nil {
			return nil, fmt.Errorf("@len: cannot get length of %T", value)
		}
		result := int64(len(list))
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
		return result, nil
	}
}

// MinOp implements @min - returns the minimum value in a list.
type MinOp struct{}

func (o *MinOp) Name() string { return "@min" }

func (o *MinOp) Evaluate(ctx *Context, args Args) (any, error) {
	list, err := getNumericList(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	if len(list) == 0 {
		return nil, fmt.Errorf("@min: empty list")
	}

	// Check if all are int.
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
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", minVal)
		return minVal, nil
	}

	minVal, _ := AsFloat(list[0])
	for i := 1; i < len(list); i++ {
		v, _ := AsFloat(list[i])
		if v < minVal {
			minVal = v
		}
	}
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", minVal)
	return minVal, nil
}

// MaxOp implements @max - returns the maximum value in a list.
type MaxOp struct{}

func (o *MaxOp) Name() string { return "@max" }

func (o *MaxOp) Evaluate(ctx *Context, args Args) (any, error) {
	list, err := getNumericList(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	if len(list) == 0 {
		return nil, fmt.Errorf("@max: empty list")
	}

	// Check if all are int.
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
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", maxVal)
		return maxVal, nil
	}

	maxVal, _ := AsFloat(list[0])
	for i := 1; i < len(list); i++ {
		v, _ := AsFloat(list[i])
		if v > maxVal {
			maxVal = v
		}
	}
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", maxVal)
	return maxVal, nil
}

// InOp implements @in - checks if an element is in a list.
// Arguments: [element, list]
type InOp struct{}

func (o *InOp) Name() string { return "@in" }

func (o *InOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@in: expected [element, list] arguments")
	}

	elemVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@in: element: %w", err)
	}

	listVal, err := listArgs.Elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@in: list: %w", err)
	}

	list, err := AsList(listVal)
	if err != nil {
		return nil, fmt.Errorf("@in: second argument must be a list: %w", err)
	}

	for _, item := range list {
		if deepEqual(elemVal, item) {
			ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", true)
			return true, nil
		}
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", false)
	return false, nil
}

// RangeOp implements @range - creates a range of integers [1..n].
type RangeOp struct{}

func (o *RangeOp) Name() string { return "@range" }

func (o *RangeOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
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

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "n", n, "result", result)
	return result, nil
}

// evaluateSingleArg evaluates a single argument from various argument types.
func evaluateSingleArg(ctx *Context, args Args, opName string) (any, error) {
	switch a := args.(type) {
	case LiteralArgs:
		return a.Value, nil
	case UnaryArgs:
		return a.Operand.Eval(ctx)
	case ListArgs:
		if len(a.Elements) != 1 {
			return nil, fmt.Errorf("%s: expected 1 argument, got %d", opName, len(a.Elements))
		}
		return a.Elements[0].Eval(ctx)
	default:
		return nil, fmt.Errorf("%s: unexpected args type %T", opName, args)
	}
}

// getNumericList evaluates arguments and returns a list of numeric values.
func getNumericList(ctx *Context, args Args, opName string) ([]any, error) {
	switch a := args.(type) {
	case ListArgs:
		result := make([]any, len(a.Elements))
		for i, elem := range a.Elements {
			v, err := elem.Eval(ctx)
			if err != nil {
				return nil, fmt.Errorf("%s[%d]: %w", opName, i, err)
			}
			if !IsNumeric(v) {
				return nil, fmt.Errorf("%s[%d]: expected numeric, got %T", opName, i, v)
			}
			result[i] = v
		}
		return result, nil
	case UnaryArgs:
		v, err := a.Operand.Eval(ctx)
		if err != nil {
			return nil, err
		}
		list, err := AsList(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", opName, err)
		}
		// Validate all elements are numeric.
		for i, item := range list {
			if !IsNumeric(item) {
				return nil, fmt.Errorf("%s[%d]: expected numeric, got %T", opName, i, item)
			}
		}
		return list, nil
	case LiteralArgs:
		list, err := AsList(a.Value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", opName, err)
		}
		for i, item := range list {
			if !IsNumeric(item) {
				return nil, fmt.Errorf("%s[%d]: expected numeric, got %T", opName, i, item)
			}
		}
		return list, nil
	default:
		return nil, fmt.Errorf("%s: unexpected args type %T", opName, args)
	}
}

func init() {
	MustRegister("@map", func() Operator { return &MapOp{} })
	MustRegister("@filter", func() Operator { return &FilterOp{} })
	MustRegister("@sum", func() Operator { return &SumOp{} })
	MustRegister("@len", func() Operator { return &LenOp{} })
	MustRegister("@min", func() Operator { return &MinOp{} })
	MustRegister("@max", func() Operator { return &MaxOp{} })
	MustRegister("@in", func() Operator { return &InOp{} })
	MustRegister("@range", func() Operator { return &RangeOp{} })
}
