package dbsp

import "fmt"

// NilOp implements @nil.
type NilOp struct{}

func (o *NilOp) Name() string { return "@nil" }

func (o *NilOp) Evaluate(ctx *Context, args Args) (any, error) {
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", nil)
	return nil, nil
}

// BoolOp implements @bool.
type BoolOp struct{}

func (o *BoolOp) Name() string { return "@bool" }

func (o *BoolOp) Evaluate(ctx *Context, args Args) (any, error) {
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
			return nil, fmt.Errorf("@bool: expected 1 argument, got %d", len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, fmt.Errorf("@bool: unexpected args type %T", args)
	}

	b, err := AsBool(value)
	if err != nil {
		return nil, fmt.Errorf("@bool: %w", err)
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", b)
	return b, nil
}

// IntOp implements @int.
type IntOp struct{}

func (o *IntOp) Name() string { return "@int" }

func (o *IntOp) Evaluate(ctx *Context, args Args) (any, error) {
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
			return nil, fmt.Errorf("@int: expected 1 argument, got %d", len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, fmt.Errorf("@int: unexpected args type %T", args)
	}

	i, err := AsInt(value)
	if err != nil {
		return nil, fmt.Errorf("@int: %w", err)
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", i)
	return i, nil
}

// FloatOp implements @float.
type FloatOp struct{}

func (o *FloatOp) Name() string { return "@float" }

func (o *FloatOp) Evaluate(ctx *Context, args Args) (any, error) {
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
			return nil, fmt.Errorf("@float: expected 1 argument, got %d", len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, fmt.Errorf("@float: unexpected args type %T", args)
	}

	f, err := AsFloat(value)
	if err != nil {
		return nil, fmt.Errorf("@float: %w", err)
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", f)
	return f, nil
}

// StringOp implements @string.
type StringOp struct{}

func (o *StringOp) Name() string { return "@string" }

func (o *StringOp) Evaluate(ctx *Context, args Args) (any, error) {
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
			return nil, fmt.Errorf("@string: expected 1 argument, got %d", len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, fmt.Errorf("@string: unexpected args type %T", args)
	}

	s, err := AsString(value)
	if err != nil {
		return nil, fmt.Errorf("@string: %w", err)
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", s)
	return s, nil
}

// ListOp implements @list - preserves nested expression structure.
type ListOp struct{}

func (o *ListOp) Name() string { return "@list" }

func (o *ListOp) Evaluate(ctx *Context, args Args) (any, error) {
	switch a := args.(type) {
	case ListArgs:
		// Evaluate each element expression.
		result := make([]any, len(a.Elements))
		for i, elem := range a.Elements {
			v, err := elem.Eval(ctx)
			if err != nil {
				return nil, fmt.Errorf("@list[%d]: %w", i, err)
			}
			result[i] = v
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
		return result, nil

	case UnaryArgs:
		// Handle nested expression that evaluates to a list.
		v, err := a.Operand.Eval(ctx)
		if err != nil {
			return nil, err
		}
		list, err := AsList(v)
		if err != nil {
			return nil, fmt.Errorf("@list: %w", err)
		}
		return list, nil

	case LiteralArgs:
		// Handle literal list.
		list, err := AsList(a.Value)
		if err != nil {
			return nil, fmt.Errorf("@list: %w", err)
		}
		return list, nil

	default:
		return nil, fmt.Errorf("@list: expected ListArgs, got %T", args)
	}
}

// DictOp implements @dict - preserves document shape while evaluating nested expressions.
type DictOp struct{}

func (o *DictOp) Name() string { return "@dict" }

func (o *DictOp) Evaluate(ctx *Context, args Args) (any, error) {
	switch a := args.(type) {
	case DictArgs:
		// Evaluate each value expression, preserving keys.
		result := make(map[string]any, len(a.Entries))
		for key, expr := range a.Entries {
			v, err := expr.Eval(ctx)
			if err != nil {
				return nil, fmt.Errorf("@dict[%q]: %w", key, err)
			}
			result[key] = v
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
		return result, nil

	case UnaryArgs:
		// Handle nested expression that evaluates to a dict.
		v, err := a.Operand.Eval(ctx)
		if err != nil {
			return nil, err
		}
		m, err := AsMap(v)
		if err != nil {
			return nil, fmt.Errorf("@dict: %w", err)
		}
		return m, nil

	case LiteralArgs:
		// Handle literal dict.
		m, err := AsMap(a.Value)
		if err != nil {
			return nil, fmt.Errorf("@dict: %w", err)
		}
		return m, nil

	default:
		return nil, fmt.Errorf("@dict: expected DictArgs, got %T", args)
	}
}

func init() {
	MustRegister("@nil", func() Operator { return &NilOp{} })
	MustRegister("@bool", func() Operator { return &BoolOp{} })
	MustRegister("@int", func() Operator { return &IntOp{} })
	MustRegister("@float", func() Operator { return &FloatOp{} })
	MustRegister("@string", func() Operator { return &StringOp{} })
	MustRegister("@list", func() Operator { return &ListOp{} })
	MustRegister("@dict", func() Operator { return &DictOp{} })
}
