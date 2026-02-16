package dbsp

import (
	"fmt"

	"github.com/l7mp/dbsp/expression"
)

// andExpr implements @and with short-circuit evaluation.
type andExpr struct{ args []Expression }

func (e *andExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if len(e.args) == 0 {
		return true, nil // Empty AND is true.
	}

	for i, elem := range e.args {
		v, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@and[%d]: %w", i, err)
		}

		b, err := AsBool(v)
		if err != nil {
			return nil, fmt.Errorf("@and[%d]: %w", i, err)
		}

		if !b {
			ctx.Logger().V(8).Info("eval", "op", "@and", "result", false, "short-circuit", i)
			return false, nil // Short-circuit.
		}
	}

	ctx.Logger().V(8).Info("eval", "op", "@and", "result", true)
	return true, nil
}

func (e *andExpr) String() string { return fmt.Sprintf("@and(%v)", e.args) }

// orExpr implements @or with short-circuit evaluation.
type orExpr struct{ args []Expression }

func (e *orExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if len(e.args) == 0 {
		return false, nil // Empty OR is false.
	}

	for i, elem := range e.args {
		v, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@or[%d]: %w", i, err)
		}

		b, err := AsBool(v)
		if err != nil {
			return nil, fmt.Errorf("@or[%d]: %w", i, err)
		}

		if b {
			ctx.Logger().V(8).Info("eval", "op", "@or", "result", true, "short-circuit", i)
			return true, nil // Short-circuit.
		}
	}

	ctx.Logger().V(8).Info("eval", "op", "@or", "result", false)
	return false, nil
}

func (e *orExpr) String() string { return fmt.Sprintf("@or(%v)", e.args) }

// notExpr implements @not.
type notExpr struct{ operand Expression }

func (e *notExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	b, err := AsBool(value)
	if err != nil {
		return nil, fmt.Errorf("@not: %w", err)
	}

	result := !b
	ctx.Logger().V(8).Info("eval", "op", "@not", "result", result)
	return result, nil
}

func (e *notExpr) String() string { return fmt.Sprintf("@not(%v)", e.operand) }

func init() {
	MustRegister("@and", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@and: %w", err)
		}
		return &andExpr{args: list}, nil
	})
	MustRegister("@or", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@or: %w", err)
		}
		return &orExpr{args: list}, nil
	})
	MustRegister("@not", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@not: %w", err)
		}
		return &notExpr{operand: operand}, nil
	})
}
