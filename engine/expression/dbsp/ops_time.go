package dbsp

import (
	"time"

	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/internal/utils"
)

// nowExpr implements @now - returns the current UTC timestamp.
type nowExpr struct{ nullaryOp }

func (e *nowExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if now, ok := ctx.Now(); ok {
		ctx.Logger().V(8).Info("eval", "op", "@now", "result", now)
		return now, nil
	}

	result := time.Now().UTC().Format(time.RFC3339)
	ctx.Logger().V(8).Info("eval", "op", "@now", "result", result)
	return result, nil
}

func init() {
	MustRegister("@now", func(args any) (Expression, error) {
		if err := utils.ValidateNullaryArgs(args, "@now"); err != nil {
			return nil, err
		}
		return &nowExpr{nullaryOp{"@now"}}, nil
	})
}
