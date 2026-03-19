package dbsp

import (
	"fmt"

	"github.com/l7mp/dbsp/dbsp/expression"
)

// isNullExpr implements @isnull.
type isNullExpr struct{ unaryOp }

func (e *isNullExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	result := value == nil
	ctx.Logger().V(8).Info("eval", "op", "@isnull", "result", result)
	return result, nil
}

func init() {
	MustRegister("@isnull", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@isnull: %w", err)
		}
		return &isNullExpr{unaryOp{"@isnull", operand}}, nil
	})
}
