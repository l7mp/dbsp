package dbsp

import (
	"time"

	"github.com/l7mp/dbsp/dbsp/expression"
)

// nowExpr implements @now - returns the current UTC timestamp.
type nowExpr struct{ nullaryOp }

func (e *nowExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	result := time.Now().UTC().Format(time.RFC3339)
	ctx.Logger().V(8).Info("eval", "op", "@now", "result", result)
	return result, nil
}

func init() {
	MustRegister("@now", func(args any) (Expression, error) {
		return &nowExpr{nullaryOp{"@now"}}, nil
	})
}
