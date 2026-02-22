package dbsp

import (
	"fmt"

	"github.com/l7mp/dbsp/expression"
)

// evaluateBinaryNumeric evaluates two sub-expressions and performs a numeric operation.
func evaluateBinaryNumeric(ctx *expression.EvalContext, left, right Expression, opName string, intOp func(a, b int64) int64, floatOp func(a, b float64) float64) (any, error) {
	aVal, err := left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: left operand: %w", opName, err)
	}

	bVal, err := right.Evaluate(ctx)
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

// evaluateVariadicNumeric evaluates multiple sub-expressions and performs a reduce operation.
func evaluateVariadicNumeric(ctx *expression.EvalContext, args []Expression, opName string, intOp func(a, b int64) int64, floatOp func(a, b float64) float64) (any, error) {
	if len(args) == 0 {
		return int64(0), nil
	}

	if len(args) == 1 {
		return args[0].Evaluate(ctx)
	}

	accVal, err := args[0].Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s[0]: %w", opName, err)
	}

	isFloat := IsFloat(accVal)

	for i := 1; i < len(args); i++ {
		nextVal, err := args[i].Evaluate(ctx)
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

// addExpr implements @add.
type addExpr struct {
	args []Expression
}

func (e *addExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	return evaluateVariadicNumeric(ctx, e.args, "@add",
		func(a, b int64) int64 { return a + b },
		func(a, b float64) float64 { return a + b },
	)
}

func (e *addExpr) String() string { return fmt.Sprintf("@add(%v)", e.args) }

// subExpr implements @sub.
type subExpr struct {
	left  Expression
	right Expression
}

func (e *subExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	return evaluateBinaryNumeric(ctx, e.left, e.right, "@sub",
		func(a, b int64) int64 { return a - b },
		func(a, b float64) float64 { return a - b },
	)
}

func (e *subExpr) String() string { return fmt.Sprintf("@sub(%v, %v)", e.left, e.right) }

// mulExpr implements @mul.
type mulExpr struct {
	args []Expression
}

func (e *mulExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	return evaluateVariadicNumeric(ctx, e.args, "@mul",
		func(a, b int64) int64 { return a * b },
		func(a, b float64) float64 { return a * b },
	)
}

func (e *mulExpr) String() string { return fmt.Sprintf("@mul(%v)", e.args) }

// divExpr implements @div.
type divExpr struct {
	left  Expression
	right Expression
}

func (e *divExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	aVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@div: left operand: %w", err)
	}

	bVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@div: right operand: %w", err)
	}

	numType, err := GetNumericType(aVal, bVal)
	if err != nil {
		return nil, fmt.Errorf("@div: %w", err)
	}

	if numType == NumericTypeInt {
		a, _ := AsInt(aVal)
		b, _ := AsInt(bVal)
		if b == 0 {
			return nil, fmt.Errorf("@div: division by zero")
		}
		result := a / b
		ctx.Logger().V(8).Info("eval", "op", "@div", "a", a, "b", b, "result", result)
		return result, nil
	}

	a, _ := AsFloat(aVal)
	b, _ := AsFloat(bVal)
	if b == 0 {
		return nil, fmt.Errorf("@div: division by zero")
	}
	result := a / b
	ctx.Logger().V(8).Info("eval", "op", "@div", "a", a, "b", b, "result", result)
	return result, nil
}

func (e *divExpr) String() string { return fmt.Sprintf("@div(%v, %v)", e.left, e.right) }

// modExpr implements @mod (modulo).
type modExpr struct {
	left  Expression
	right Expression
}

func (e *modExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	aVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@mod: left operand: %w", err)
	}

	bVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@mod: right operand: %w", err)
	}

	a, err := AsInt(aVal)
	if err != nil {
		return nil, fmt.Errorf("@mod: left operand: %w", err)
	}

	b, err := AsInt(bVal)
	if err != nil {
		return nil, fmt.Errorf("@mod: right operand: %w", err)
	}

	if b == 0 {
		return nil, fmt.Errorf("@mod: division by zero")
	}

	result := a % b
	ctx.Logger().V(8).Info("eval", "op", "@mod", "a", a, "b", b, "result", result)
	return result, nil
}

func (e *modExpr) String() string { return fmt.Sprintf("@mod(%v, %v)", e.left, e.right) }

// negExpr implements @neg (unary negation).
type negExpr struct {
	operand Expression
}

func (e *negExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	if IsInt(value) {
		i, _ := AsInt(value)
		result := -i
		ctx.Logger().V(8).Info("eval", "op", "@neg", "result", result)
		return result, nil
	}

	f, err := AsFloat(value)
	if err != nil {
		return nil, fmt.Errorf("@neg: %w", err)
	}
	result := -f
	ctx.Logger().V(8).Info("eval", "op", "@neg", "result", result)
	return result, nil
}

func (e *negExpr) String() string { return fmt.Sprintf("@neg(%v)", e.operand) }

func init() {
	MustRegister("@add", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@add: %w", err)
		}
		return &addExpr{args: list}, nil
	})
	MustRegister("@sub", func(args any) (Expression, error) {
		left, right, err := asBinaryExprs(args, "@sub")
		if err != nil {
			return nil, err
		}
		return &subExpr{left: left, right: right}, nil
	})
	MustRegister("@mul", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@mul: %w", err)
		}
		return &mulExpr{args: list}, nil
	})
	MustRegister("@div", func(args any) (Expression, error) {
		left, right, err := asBinaryExprs(args, "@div")
		if err != nil {
			return nil, err
		}
		return &divExpr{left: left, right: right}, nil
	})
	MustRegister("@mod", func(args any) (Expression, error) {
		left, right, err := asBinaryExprs(args, "@mod")
		if err != nil {
			return nil, err
		}
		return &modExpr{left: left, right: right}, nil
	})
	MustRegister("@neg", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@neg: %w", err)
		}
		return &negExpr{operand: operand}, nil
	})
}

// asExprListOrSingle converts args to []Expression, accepting a single Expression or []Expression.
func asExprListOrSingle(args any) ([]Expression, error) {
	if list, ok := args.([]Expression); ok {
		return list, nil
	}
	if e, ok := args.(Expression); ok {
		return []Expression{e}, nil
	}
	return nil, fmt.Errorf("expected []Expression or Expression, got %T", args)
}

// asBinaryExprs extracts exactly two expressions from args.
func asBinaryExprs(args any, opName string) (Expression, Expression, error) {
	list, ok := args.([]Expression)
	if !ok || len(list) != 2 {
		return nil, nil, fmt.Errorf("%s: expected 2 arguments", opName)
	}
	return list[0], list[1], nil
}

// asUnaryExprOrLiteral converts args to a single Expression, wrapping literal values.
func asUnaryExprOrLiteral(args any) (Expression, error) {
	if e, ok := args.(Expression); ok {
		return e, nil
	}
	if list, ok := args.([]Expression); ok {
		if len(list) == 1 {
			return list[0], nil
		}
		return nil, fmt.Errorf("expected 1 argument, got %d", len(list))
	}
	// Wrap literal value.
	return &constExpr{value: args}, nil
}
