package dbsp

import "fmt"

// SqlBoolOp implements @sqlbool to normalize SQL boolean semantics.
// It maps nil to false, propagating only true when the operand is true.
type SqlBoolOp struct{}

func (o *SqlBoolOp) Name() string { return "@sqlbool" }

func (o *SqlBoolOp) Evaluate(ctx *Context, args Args) (any, error) {
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
			return nil, fmt.Errorf("@sqlbool: expected 1 argument, got %d", len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, fmt.Errorf("@sqlbool: unexpected args type %T", args)
	}

	if value == nil {
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", false)
		return false, nil
	}
	result, err := AsBool(value)
	if err != nil {
		return nil, fmt.Errorf("@sqlbool: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

func init() {
	MustRegister("@sqlbool", func() Operator { return &SqlBoolOp{} })
}
