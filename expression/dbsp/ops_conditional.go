package dbsp

import "fmt"

// CondOp implements @cond - conditional expression.
// Arguments: [condition, if-true, if-false]
type CondOp struct{}

func (o *CondOp) Name() string { return "@cond" }

func (o *CondOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 3 {
		return nil, fmt.Errorf("@cond: expected [condition, if-true, if-false] arguments")
	}

	// Evaluate condition.
	condVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@cond: condition: %w", err)
	}

	cond, err := AsBool(condVal)
	if err != nil {
		return nil, fmt.Errorf("@cond: condition must be bool: %w", err)
	}

	// Evaluate only the appropriate branch (short-circuit).
	if cond {
		result, err := listArgs.Elements[1].Eval(ctx)
		if err != nil {
			return nil, fmt.Errorf("@cond: if-true: %w", err)
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "condition", true, "result", result)
		return result, nil
	}

	result, err := listArgs.Elements[2].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@cond: if-false: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "condition", false, "result", result)
	return result, nil
}

// SwitchOp implements @switch - pattern matching.
// Arguments: [[case1, expr1], [case2, expr2], ...]
// Each case is evaluated in order; first match wins.
// A case of true or @bool(true) acts as default.
type SwitchOp struct{}

func (o *SwitchOp) Name() string { return "@switch" }

func (o *SwitchOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok {
		return nil, fmt.Errorf("@switch: expected list of [case, expr] pairs")
	}

	for i, elem := range listArgs.Elements {
		// Each element should be a list of [case, expr].
		pairVal, err := elem.Eval(ctx)
		if err != nil {
			// Try treating it as a pair expression directly.
			pairArgs := elem.Args()
			pair, ok := pairArgs.(ListArgs)
			if !ok || len(pair.Elements) != 2 {
				return nil, fmt.Errorf("@switch[%d]: expected [case, expr] pair: %w", i, err)
			}

			// Evaluate case.
			caseVal, err := pair.Elements[0].Eval(ctx)
			if err != nil {
				return nil, fmt.Errorf("@switch[%d]: case: %w", i, err)
			}

			caseBool, err := AsBool(caseVal)
			if err != nil {
				return nil, fmt.Errorf("@switch[%d]: case must be bool: %w", i, err)
			}

			if caseBool {
				result, err := pair.Elements[1].Eval(ctx)
				if err != nil {
					return nil, fmt.Errorf("@switch[%d]: expr: %w", i, err)
				}
				ctx.Logger().V(8).Info("eval", "op", o.Name(), "match", i, "result", result)
				return result, nil
			}
			continue
		}

		// If evaluated, it should be a list.
		pairList, err := AsList(pairVal)
		if err != nil || len(pairList) != 2 {
			return nil, fmt.Errorf("@switch[%d]: expected [case, expr] pair", i)
		}

		caseBool, err := AsBool(pairList[0])
		if err != nil {
			return nil, fmt.Errorf("@switch[%d]: case must be bool: %w", i, err)
		}

		if caseBool {
			ctx.Logger().V(8).Info("eval", "op", o.Name(), "match", i, "result", pairList[1])
			return pairList[1], nil
		}
	}

	// No match found.
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "match", -1, "result", nil)
	return nil, nil
}

// DefinedOrOp implements @definedOr - returns the first non-nil value.
// Arguments: [expr1, expr2, ...]
type DefinedOrOp struct{}

func (o *DefinedOrOp) Name() string { return "@definedOr" }

func (o *DefinedOrOp) Evaluate(ctx *Context, args Args) (any, error) {
	elements, err := getElements(args, o.Name())
	if err != nil {
		return nil, err
	}

	for i, elem := range elements {
		v, err := elem.Eval(ctx)
		if err != nil {
			// Treat errors as "not defined".
			ctx.Logger().V(8).Info("eval", "op", o.Name(), "index", i, "error", err)
			continue
		}
		if v != nil {
			ctx.Logger().V(8).Info("eval", "op", o.Name(), "match", i, "result", v)
			return v, nil
		}
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", nil)
	return nil, nil
}

func init() {
	MustRegister("@cond", func() Operator { return &CondOp{} })
	MustRegister("@switch", func() Operator { return &SwitchOp{} })
	MustRegister("@definedOr", func() Operator { return &DefinedOrOp{} })
}
