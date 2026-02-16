package dbsp

import (
	"time"

	"github.com/l7mp/dbsp/expression"
)

// nowExpr implements @now - returns the current UTC timestamp.
type nowExpr struct{}

func (e *nowExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	result := time.Now().UTC().Format(time.RFC3339)
	ctx.Logger().V(8).Info("eval", "op", "@now", "result", result)
	return result, nil
}

func (e *nowExpr) String() string { return "@now" }

func init() {
	MustRegister("@now", func(args any) (Expression, error) {
		return &nowExpr{}, nil
	})
}
