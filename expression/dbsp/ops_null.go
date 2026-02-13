package dbsp

import "fmt"

// IsNullOp implements @isnull.
type IsNullOp struct{}

func (o *IsNullOp) Name() string { return "@isnull" }

func (o *IsNullOp) Evaluate(ctx *Context, args Args) (any, error) {
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
			return nil, fmt.Errorf("@isnull: expected 1 argument, got %d", len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return nil, err
		}
		value = v
	default:
		return nil, fmt.Errorf("@isnull: unexpected args type %T", args)
	}

	result := value == nil
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

func init() {
	MustRegister("@isnull", func() Operator { return &IsNullOp{} })
}
