package dbsp

import (
	"fmt"

	"github.com/l7mp/dbsp/expression"
)

// nilExpr implements @nil.
type nilExpr struct{}

func (e *nilExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	ctx.Logger().V(8).Info("eval", "op", "@nil", "result", nil)
	return nil, nil
}

func (e *nilExpr) String() string { return "@nil" }

// boolExpr implements @bool.
type boolExpr struct{ operand Expression }

func (e *boolExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if e.operand == nil {
		return false, nil
	}
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	b, err := AsBool(value)
	if err != nil {
		return nil, fmt.Errorf("@bool: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", "@bool", "result", b)
	return b, nil
}

func (e *boolExpr) String() string { return fmt.Sprintf("@bool(%v)", e.operand) }

// intExpr implements @int.
type intExpr struct{ operand Expression }

func (e *intExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if e.operand == nil {
		return int64(0), nil
	}
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	i, err := AsInt(value)
	if err != nil {
		return nil, fmt.Errorf("@int: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", "@int", "result", i)
	return i, nil
}

func (e *intExpr) String() string { return fmt.Sprintf("@int(%v)", e.operand) }

// floatExpr implements @float.
type floatExpr struct{ operand Expression }

func (e *floatExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if e.operand == nil {
		return float64(0), nil
	}
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	f, err := AsFloat(value)
	if err != nil {
		return nil, fmt.Errorf("@float: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", "@float", "result", f)
	return f, nil
}

func (e *floatExpr) String() string { return fmt.Sprintf("@float(%v)", e.operand) }

// stringExpr implements @string.
type stringExpr struct{ operand Expression }

func (e *stringExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if e.operand == nil {
		return "", nil
	}
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	s, err := AsString(value)
	if err != nil {
		return nil, fmt.Errorf("@string: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", "@string", "result", s)
	return s, nil
}

func (e *stringExpr) String() string { return fmt.Sprintf("@string(%v)", e.operand) }

// listExpr implements @list - evaluates each element expression.
type listExpr struct{ elements []Expression }

func (e *listExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	result := make([]any, len(e.elements))
	for i, elem := range e.elements {
		v, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@list[%d]: %w", i, err)
		}
		result[i] = v
	}
	ctx.Logger().V(8).Info("eval", "op", "@list", "result", result)
	return result, nil
}

func (e *listExpr) String() string { return fmt.Sprintf("@list(%v)", e.elements) }

// dictExpr implements @dict - evaluates each value expression, preserving keys.
type dictExpr struct{ entries map[string]Expression }

func (e *dictExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	result := make(map[string]any, len(e.entries))
	for key, expr := range e.entries {
		v, err := expr.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@dict[%q]: %w", key, err)
		}
		result[key] = v
	}
	ctx.Logger().V(8).Info("eval", "op", "@dict", "result", result)
	return result, nil
}

func (e *dictExpr) String() string { return fmt.Sprintf("@dict(%v)", e.entries) }

// constExpr wraps a literal value as an expression.
type constExpr struct{ value any }

func (e *constExpr) Evaluate(_ *expression.EvalContext) (any, error) {
	return e.value, nil
}

func (e *constExpr) String() string { return fmt.Sprintf("%v", e.value) }

func init() {
	MustRegister("@nil", func(args any) (Expression, error) {
		return &nilExpr{}, nil
	})
	MustRegister("@bool", func(args any) (Expression, error) {
		// Literal bool value.
		if b, ok := args.(bool); ok {
			return &boolExpr{operand: &constExpr{value: b}}, nil
		}
		// Sub-expression.
		if e, ok := args.(Expression); ok {
			return &boolExpr{operand: e}, nil
		}
		if args == nil {
			return &boolExpr{operand: &constExpr{value: false}}, nil
		}
		return nil, fmt.Errorf("@bool: unexpected args type %T", args)
	})
	MustRegister("@int", func(args any) (Expression, error) {
		if e, ok := args.(Expression); ok {
			return &intExpr{operand: e}, nil
		}
		// Literal numeric value.
		return &intExpr{operand: &constExpr{value: args}}, nil
	})
	MustRegister("@float", func(args any) (Expression, error) {
		if e, ok := args.(Expression); ok {
			return &floatExpr{operand: e}, nil
		}
		return &floatExpr{operand: &constExpr{value: args}}, nil
	})
	MustRegister("@string", func(args any) (Expression, error) {
		if e, ok := args.(Expression); ok {
			return &stringExpr{operand: e}, nil
		}
		return &stringExpr{operand: &constExpr{value: args}}, nil
	})
	MustRegister("@list", func(args any) (Expression, error) {
		if list, ok := args.([]Expression); ok {
			return &listExpr{elements: list}, nil
		}
		if e, ok := args.(Expression); ok {
			return &listExpr{elements: []Expression{e}}, nil
		}
		return &listExpr{elements: nil}, nil
	})
	MustRegister("@dict", func(args any) (Expression, error) {
		if m, ok := args.(map[string]Expression); ok {
			return &dictExpr{entries: m}, nil
		}
		if e, ok := args.(Expression); ok {
			// Single expression that should evaluate to a map.
			return &dictExpr{entries: map[string]Expression{"": e}}, nil
		}
		return &dictExpr{entries: nil}, nil
	})
}
