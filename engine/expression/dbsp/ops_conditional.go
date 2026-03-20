package dbsp

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/expression"
)

// condExpr implements @cond - conditional expression.
// Arguments: [condition, if-true, if-false].
type condExpr struct {
	cond    Expression
	ifTrue  Expression
	ifFalse Expression
}

func (e *condExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	condVal, err := e.cond.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@cond: condition: %w", err)
	}

	cond, err := AsBool(condVal)
	if err != nil {
		return nil, fmt.Errorf("@cond: condition must be bool: %w", err)
	}

	if cond {
		result, err := e.ifTrue.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@cond: if-true: %w", err)
		}
		ctx.Logger().V(8).Info("eval", "op", "@cond", "condition", true, "result", result)
		return result, nil
	}

	result, err := e.ifFalse.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@cond: if-false: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", "@cond", "condition", false, "result", result)
	return result, nil
}

func (e *condExpr) String() string {
	return fmt.Sprintf("@cond(%v, %v, %v)", e.cond, e.ifTrue, e.ifFalse)
}

// switchExpr implements @switch - pattern matching.
// Each element is a [case, expr] pair expression.
type switchExpr struct{ variadicOp }

func (e *switchExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	for i, elem := range e.args {
		pairVal, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@switch[%d]: %w", i, err)
		}

		pairList, err := AsList(pairVal)
		if err != nil || len(pairList) != 2 {
			return nil, fmt.Errorf("@switch[%d]: expected [case, expr] pair", i)
		}

		caseBool, err := AsBool(pairList[0])
		if err != nil {
			return nil, fmt.Errorf("@switch[%d]: case must be bool: %w", i, err)
		}

		if caseBool {
			ctx.Logger().V(8).Info("eval", "op", "@switch", "match", i, "result", pairList[1])
			return pairList[1], nil
		}
	}

	ctx.Logger().V(8).Info("eval", "op", "@switch", "match", -1, "result", nil)
	return nil, nil
}

// definedOrExpr implements @definedOr - returns the first non-nil value.
type definedOrExpr struct{ variadicOp }

func (e *definedOrExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	for i, elem := range e.args {
		v, err := elem.Evaluate(ctx)
		if err != nil {
			ctx.Logger().V(8).Info("eval", "op", "@definedOr", "index", i, "error", err)
			continue
		}
		if v != nil {
			ctx.Logger().V(8).Info("eval", "op", "@definedOr", "match", i, "result", v)
			return v, nil
		}
	}

	ctx.Logger().V(8).Info("eval", "op", "@definedOr", "result", nil)
	return nil, nil
}

func init() {
	MustRegister("@cond", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || len(list) != 3 {
			return nil, fmt.Errorf("@cond: expected [condition, if-true, if-false] arguments")
		}
		return &condExpr{cond: list[0], ifTrue: list[1], ifFalse: list[2]}, nil
	})
	MustRegister("@switch", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@switch: %w", err)
		}
		return &switchExpr{variadicOp{"@switch", list}}, nil
	})
	MustRegister("@definedOr", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@definedOr: %w", err)
		}
		return &definedOrExpr{variadicOp{"@definedOr", list}}, nil
	})
}
