package dbsp

import "fmt"

// AndOp implements @and with short-circuit evaluation.
type AndOp struct{}

func (o *AndOp) Name() string { return "@and" }

func (o *AndOp) Evaluate(ctx *Context, args Args) (any, error) {
	elements, err := o.getElements(args)
	if err != nil {
		return nil, err
	}

	if len(elements) == 0 {
		return true, nil // Empty AND is true.
	}

	for i, elem := range elements {
		v, err := elem.Eval(ctx)
		if err != nil {
			return nil, fmt.Errorf("@and[%d]: %w", i, err)
		}

		b, err := AsBool(v)
		if err != nil {
			return nil, fmt.Errorf("@and[%d]: %w", i, err)
		}

		if !b {
			ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", false, "short-circuit", i)
			return false, nil // Short-circuit.
		}
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", true)
	return true, nil
}

func (o *AndOp) getElements(args Args) ([]Expr, error) {
	switch a := args.(type) {
	case ListArgs:
		return a.Elements, nil
	case UnaryArgs:
		return []Expr{a.Operand}, nil
	default:
		return nil, fmt.Errorf("@and: expected ListArgs or UnaryArgs, got %T", args)
	}
}

// OrOp implements @or with short-circuit evaluation.
type OrOp struct{}

func (o *OrOp) Name() string { return "@or" }

func (o *OrOp) Evaluate(ctx *Context, args Args) (any, error) {
	elements, err := o.getElements(args)
	if err != nil {
		return nil, err
	}

	if len(elements) == 0 {
		return false, nil // Empty OR is false.
	}

	for i, elem := range elements {
		v, err := elem.Eval(ctx)
		if err != nil {
			return nil, fmt.Errorf("@or[%d]: %w", i, err)
		}

		b, err := AsBool(v)
		if err != nil {
			return nil, fmt.Errorf("@or[%d]: %w", i, err)
		}

		if b {
			ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", true, "short-circuit", i)
			return true, nil // Short-circuit.
		}
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", false)
	return false, nil
}

func (o *OrOp) getElements(args Args) ([]Expr, error) {
	switch a := args.(type) {
	case ListArgs:
		return a.Elements, nil
	case UnaryArgs:
		return []Expr{a.Operand}, nil
	default:
		return nil, fmt.Errorf("@or: expected ListArgs or UnaryArgs, got %T", args)
	}
}

// NotOp implements @not.
type NotOp struct{}

func (o *NotOp) Name() string { return "@not" }

func (o *NotOp) Evaluate(ctx *Context, args Args) (any, error) {
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
			return nil, fmt.Errorf("@not: expected 1 argument, got %d", len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, fmt.Errorf("@not: unexpected args type %T", args)
	}

	b, err := AsBool(value)
	if err != nil {
		return nil, fmt.Errorf("@not: %w", err)
	}

	result := !b
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

func init() {
	MustRegister("@and", func() Operator { return &AndOp{} })
	MustRegister("@or", func() Operator { return &OrOp{} })
	MustRegister("@not", func() Operator { return &NotOp{} })
}
