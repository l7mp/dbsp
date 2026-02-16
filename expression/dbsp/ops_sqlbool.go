package dbsp

import (
	"fmt"

	"github.com/l7mp/dbsp/expression"
)

// sqlBoolExpr implements @sqlbool to normalize SQL boolean semantics.
// It maps nil to false, propagating only true when the operand is true.
type sqlBoolExpr struct{ operand Expression }

func (e *sqlBoolExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	if value == nil {
		ctx.Logger().V(8).Info("eval", "op", "@sqlbool", "result", false)
		return false, nil
	}
	result, err := AsBool(value)
	if err != nil {
		return nil, fmt.Errorf("@sqlbool: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", "@sqlbool", "result", result)
	return result, nil
}

func (e *sqlBoolExpr) String() string { return fmt.Sprintf("@sqlbool(%v)", e.operand) }

func init() {
	MustRegister("@sqlbool", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@sqlbool: %w", err)
		}
		return &sqlBoolExpr{operand: operand}, nil
	})
}
