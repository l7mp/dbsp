package dbsp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"

	"github.com/l7mp/dbsp/expression"
)

// noopExpr implements @noop - returns nil.
type noopExpr struct{}

func (e *noopExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	ctx.Logger().V(8).Info("eval", "op", "@noop")
	return nil, nil
}

func (e *noopExpr) String() string { return "@noop" }

// argExpr implements @arg - returns the current subject from context.
type argExpr struct{}

func (e *argExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	subject := ctx.Subject()
	ctx.Logger().V(8).Info("eval", "op", "@arg", "result", subject)
	return subject, nil
}

func (e *argExpr) String() string { return "@arg" }

// hashExpr implements @hash - creates a deterministic hash of the argument.
type hashExpr struct {
	operand Expression
}

func (e *hashExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("@hash: cannot serialize value: %w", err)
	}

	hash := sha256.Sum256(data)
	result := hex.EncodeToString(hash[:8])

	ctx.Logger().V(8).Info("eval", "op", "@hash", "result", result)
	return result, nil
}

func (e *hashExpr) String() string { return fmt.Sprintf("@hash(%v)", e.operand) }

// rndExpr implements @rnd - returns a random number in [min, max].
type rndExpr struct {
	min Expression
	max Expression
}

func (e *rndExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	minVal, err := e.min.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@rnd: min: %w", err)
	}

	maxVal, err := e.max.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@rnd: max: %w", err)
	}

	if IsInt(minVal) && IsInt(maxVal) {
		minInt, _ := AsInt(minVal)
		maxInt, _ := AsInt(maxVal)
		if minInt > maxInt {
			return nil, fmt.Errorf("@rnd: min > max")
		}
		result := minInt + rand.Int64N(maxInt-minInt+1)
		ctx.Logger().V(8).Info("eval", "op", "@rnd", "min", minInt, "max", maxInt, "result", result)
		return result, nil
	}

	minFloat, err := AsFloat(minVal)
	if err != nil {
		return nil, fmt.Errorf("@rnd: min: %w", err)
	}
	maxFloat, err := AsFloat(maxVal)
	if err != nil {
		return nil, fmt.Errorf("@rnd: max: %w", err)
	}
	if minFloat > maxFloat {
		return nil, fmt.Errorf("@rnd: min > max")
	}

	result := minFloat + rand.Float64()*(maxFloat-minFloat)
	ctx.Logger().V(8).Info("eval", "op", "@rnd", "min", minFloat, "max", maxFloat, "result", result)
	return result, nil
}

func (e *rndExpr) String() string { return fmt.Sprintf("@rnd(%v, %v)", e.min, e.max) }

// concatExpr implements @concat - concatenates strings.
type concatExpr struct {
	args []Expression
}

func (e *concatExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	var result string
	for i, elem := range e.args {
		v, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@concat[%d]: %w", i, err)
		}
		s, err := AsString(v)
		if err != nil {
			return nil, fmt.Errorf("@concat[%d]: %w", i, err)
		}
		result += s
	}

	ctx.Logger().V(8).Info("eval", "op", "@concat", "result", result)
	return result, nil
}

func (e *concatExpr) String() string { return fmt.Sprintf("@concat(%v)", e.args) }

// absExpr implements @abs - returns the absolute value.
type absExpr struct {
	operand Expression
}

func (e *absExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	switch v := value.(type) {
	case int:
		if v < 0 {
			v = -v
		}
		ctx.Logger().V(8).Info("eval", "op", "@abs", "result", v)
		return int64(v), nil
	case int64:
		if v < 0 {
			v = -v
		}
		ctx.Logger().V(8).Info("eval", "op", "@abs", "result", v)
		return v, nil
	case float64:
		if v < 0 {
			v = -v
		}
		ctx.Logger().V(8).Info("eval", "op", "@abs", "result", v)
		return v, nil
	case float32:
		if v < 0 {
			v = -v
		}
		ctx.Logger().V(8).Info("eval", "op", "@abs", "result", v)
		return float64(v), nil
	}

	if IsInt(value) {
		i, _ := AsInt(value)
		if i < 0 {
			i = -i
		}
		ctx.Logger().V(8).Info("eval", "op", "@abs", "result", i)
		return i, nil
	}

	f, err := AsFloat(value)
	if err != nil {
		return nil, fmt.Errorf("@abs: %w", err)
	}
	if f < 0 {
		f = -f
	}
	ctx.Logger().V(8).Info("eval", "op", "@abs", "result", f)
	return f, nil
}

func (e *absExpr) String() string { return fmt.Sprintf("@abs(%v)", e.operand) }

// floorExpr implements @floor - rounds down to nearest integer.
type floorExpr struct {
	operand Expression
}

func (e *floorExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	f, err := AsFloat(value)
	if err != nil {
		return nil, fmt.Errorf("@floor: %w", err)
	}

	result := int64(f)
	if f < 0 && f != float64(result) {
		result--
	}
	ctx.Logger().V(8).Info("eval", "op", "@floor", "result", result)
	return result, nil
}

func (e *floorExpr) String() string { return fmt.Sprintf("@floor(%v)", e.operand) }

// ceilExpr implements @ceil - rounds up to nearest integer.
type ceilExpr struct {
	operand Expression
}

func (e *ceilExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	f, err := AsFloat(value)
	if err != nil {
		return nil, fmt.Errorf("@ceil: %w", err)
	}

	result := int64(f)
	if f > 0 && f != float64(result) {
		result++
	}
	ctx.Logger().V(8).Info("eval", "op", "@ceil", "result", result)
	return result, nil
}

func (e *ceilExpr) String() string { return fmt.Sprintf("@ceil(%v)", e.operand) }

// isNilExpr implements @isnil - checks if a value is nil.
type isNilExpr struct {
	operand Expression
}

func (e *isNilExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	result := value == nil
	ctx.Logger().V(8).Info("eval", "op", "@isnil", "result", result)
	return result, nil
}

func (e *isNilExpr) String() string { return fmt.Sprintf("@isnil(%v)", e.operand) }

func init() {
	MustRegister("@noop", func(args any) (Expression, error) {
		return &noopExpr{}, nil
	})
	MustRegister("@arg", func(args any) (Expression, error) {
		return &argExpr{}, nil
	})
	MustRegister("@hash", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@hash: %w", err)
		}
		return &hashExpr{operand: operand}, nil
	})
	MustRegister("@rnd", func(args any) (Expression, error) {
		left, right, err := asBinaryExprs(args, "@rnd")
		if err != nil {
			return nil, err
		}
		return &rndExpr{min: left, max: right}, nil
	})
	MustRegister("@concat", func(args any) (Expression, error) {
		list, err := asExprListOrSingle(args)
		if err != nil {
			return nil, fmt.Errorf("@concat: %w", err)
		}
		return &concatExpr{args: list}, nil
	})
	MustRegister("@abs", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@abs: %w", err)
		}
		return &absExpr{operand: operand}, nil
	})
	MustRegister("@floor", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@floor: %w", err)
		}
		return &floorExpr{operand: operand}, nil
	})
	MustRegister("@ceil", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@ceil: %w", err)
		}
		return &ceilExpr{operand: operand}, nil
	})
	MustRegister("@isnil", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@isnil: %w", err)
		}
		return &isNilExpr{operand: operand}, nil
	})
}
