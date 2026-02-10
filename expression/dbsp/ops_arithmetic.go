package dbsp

import "fmt"

// evaluateBinaryNumeric evaluates two arguments and performs a numeric operation.
// If both operands are integers, performs integer arithmetic; otherwise float.
func evaluateBinaryNumeric(ctx *Context, args Args, opName string, intOp func(a, b int64) int64, floatOp func(a, b float64) float64) (any, error) {
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

	numType, err := GetNumericType(aVal, bVal)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", opName, err)
	}

	if numType == NumericTypeInt {
		a, _ := AsInt(aVal)
		b, _ := AsInt(bVal)
		result := intOp(a, b)
		ctx.Logger().V(8).Info("eval", "op", opName, "a", a, "b", b, "result", result)
		return result, nil
	}

	a, _ := AsFloat(aVal)
	b, _ := AsFloat(bVal)
	result := floatOp(a, b)
	ctx.Logger().V(8).Info("eval", "op", opName, "a", a, "b", b, "result", result)
	return result, nil
}

// evaluateVariadicNumeric evaluates multiple arguments and performs a reduce operation.
func evaluateVariadicNumeric(ctx *Context, args Args, opName string, intOp func(a, b int64) int64, floatOp func(a, b float64) float64) (any, error) {
	elements, err := getElements(args, opName)
	if err != nil {
		return nil, err
	}

	if len(elements) == 0 {
		return int64(0), nil
	}

	if len(elements) == 1 {
		return elements[0].Eval(ctx)
	}

	// Evaluate first element to determine initial type.
	accVal, err := elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s[0]: %w", opName, err)
	}

	isFloat := IsFloat(accVal)

	for i := 1; i < len(elements); i++ {
		nextVal, err := elements[i].Eval(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: %w", opName, i, err)
		}

		if IsFloat(nextVal) {
			isFloat = true
		}

		if isFloat {
			a, err := AsFloat(accVal)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", opName, err)
			}
			b, err := AsFloat(nextVal)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", opName, err)
			}
			accVal = floatOp(a, b)
		} else {
			a, err := AsInt(accVal)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", opName, err)
			}
			b, err := AsInt(nextVal)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", opName, err)
			}
			accVal = intOp(a, b)
		}
	}

	ctx.Logger().V(8).Info("eval", "op", opName, "result", accVal)
	return accVal, nil
}

func getBinaryElements(args Args, opName string) ([]Expr, error) {
	switch a := args.(type) {
	case ListArgs:
		if len(a.Elements) != 2 {
			return nil, fmt.Errorf("%s: expected 2 arguments, got %d", opName, len(a.Elements))
		}
		return a.Elements, nil
	default:
		return nil, fmt.Errorf("%s: expected ListArgs, got %T", opName, args)
	}
}

func getElements(args Args, opName string) ([]Expr, error) {
	switch a := args.(type) {
	case ListArgs:
		return a.Elements, nil
	case UnaryArgs:
		return []Expr{a.Operand}, nil
	default:
		return nil, fmt.Errorf("%s: expected ListArgs or UnaryArgs, got %T", opName, args)
	}
}

// AddOp implements @add.
type AddOp struct{}

func (o *AddOp) Name() string { return "@add" }

func (o *AddOp) Evaluate(ctx *Context, args Args) (any, error) {
	return evaluateVariadicNumeric(ctx, args, o.Name(),
		func(a, b int64) int64 { return a + b },
		func(a, b float64) float64 { return a + b },
	)
}

// SubOp implements @sub.
type SubOp struct{}

func (o *SubOp) Name() string { return "@sub" }

func (o *SubOp) Evaluate(ctx *Context, args Args) (any, error) {
	return evaluateBinaryNumeric(ctx, args, o.Name(),
		func(a, b int64) int64 { return a - b },
		func(a, b float64) float64 { return a - b },
	)
}

// MulOp implements @mul.
type MulOp struct{}

func (o *MulOp) Name() string { return "@mul" }

func (o *MulOp) Evaluate(ctx *Context, args Args) (any, error) {
	return evaluateVariadicNumeric(ctx, args, o.Name(),
		func(a, b int64) int64 { return a * b },
		func(a, b float64) float64 { return a * b },
	)
}

// DivOp implements @div.
type DivOp struct{}

func (o *DivOp) Name() string { return "@div" }

func (o *DivOp) Evaluate(ctx *Context, args Args) (any, error) {
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

	numType, err := GetNumericType(aVal, bVal)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", o.Name(), err)
	}

	if numType == NumericTypeInt {
		a, _ := AsInt(aVal)
		b, _ := AsInt(bVal)
		if b == 0 {
			return nil, fmt.Errorf("%s: division by zero", o.Name())
		}
		result := a / b
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "a", a, "b", b, "result", result)
		return result, nil
	}

	a, _ := AsFloat(aVal)
	b, _ := AsFloat(bVal)
	if b == 0 {
		return nil, fmt.Errorf("%s: division by zero", o.Name())
	}
	result := a / b
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "a", a, "b", b, "result", result)
	return result, nil
}

// ModOp implements @mod (modulo).
type ModOp struct{}

func (o *ModOp) Name() string { return "@mod" }

func (o *ModOp) Evaluate(ctx *Context, args Args) (any, error) {
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

	a, err := AsInt(aVal)
	if err != nil {
		return nil, fmt.Errorf("%s: left operand: %w", o.Name(), err)
	}

	b, err := AsInt(bVal)
	if err != nil {
		return nil, fmt.Errorf("%s: right operand: %w", o.Name(), err)
	}

	if b == 0 {
		return nil, fmt.Errorf("%s: division by zero", o.Name())
	}

	result := a % b
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "a", a, "b", b, "result", result)
	return result, nil
}

// NegOp implements @neg (unary negation).
type NegOp struct{}

func (o *NegOp) Name() string { return "@neg" }

func (o *NegOp) Evaluate(ctx *Context, args Args) (any, error) {
	var value any

	switch a := args.(type) {
	case LiteralArgs:
		value = a.Value
	case UnaryArgs:
		v, err := a.Operand.Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	case ListArgs:
		if len(a.Elements) != 1 {
			return nil, fmt.Errorf("@neg: expected 1 argument, got %d", len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, fmt.Errorf("@neg: unexpected args type %T", args)
	}

	if IsInt(value) {
		i, _ := AsInt(value)
		result := -i
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
		return result, nil
	}

	f, err := AsFloat(value)
	if err != nil {
		return nil, fmt.Errorf("@neg: %w", err)
	}
	result := -f
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

func init() {
	MustRegister("@add", func() Operator { return &AddOp{} })
	MustRegister("@sub", func() Operator { return &SubOp{} })
	MustRegister("@mul", func() Operator { return &MulOp{} })
	MustRegister("@div", func() Operator { return &DivOp{} })
	MustRegister("@mod", func() Operator { return &ModOp{} })
	MustRegister("@neg", func() Operator { return &NegOp{} })
}
