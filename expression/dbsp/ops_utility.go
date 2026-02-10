package dbsp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
)

// NoopOp implements @noop - returns nil.
type NoopOp struct{}

func (o *NoopOp) Name() string { return "@noop" }

func (o *NoopOp) Evaluate(ctx *Context, args Args) (any, error) {
	ctx.Logger().V(8).Info("eval", "op", o.Name())
	return nil, nil
}

// ArgOp implements @arg - returns the current subject from context.
// In @map/@filter, this is the current iteration element.
// Future: may also be used to modify operator behavior (hash length, rnd algorithm, etc.).
type ArgOp struct{}

func (o *ArgOp) Name() string { return "@arg" }

func (o *ArgOp) Evaluate(ctx *Context, args Args) (any, error) {
	subject := ctx.Subject()
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", subject)
	return subject, nil
}

// HashOp implements @hash - creates a deterministic hash of the argument.
type HashOp struct{}

func (o *HashOp) Name() string { return "@hash" }

func (o *HashOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	// Serialize to JSON for consistent hashing.
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("@hash: cannot serialize value: %w", err)
	}

	hash := sha256.Sum256(data)
	result := hex.EncodeToString(hash[:8]) // First 8 bytes = 16 hex chars.

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// RndOp implements @rnd - returns a random number in [min, max].
// Arguments: [min, max]
type RndOp struct{}

func (o *RndOp) Name() string { return "@rnd" }

func (o *RndOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@rnd: expected [min, max] arguments")
	}

	minVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@rnd: min: %w", err)
	}

	maxVal, err := listArgs.Elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@rnd: max: %w", err)
	}

	// Check if both are int.
	if IsInt(minVal) && IsInt(maxVal) {
		minInt, _ := AsInt(minVal)
		maxInt, _ := AsInt(maxVal)
		if minInt > maxInt {
			return nil, fmt.Errorf("@rnd: min > max")
		}
		result := minInt + rand.Int64N(maxInt-minInt+1)
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "min", minInt, "max", maxInt, "result", result)
		return result, nil
	}

	// Otherwise use float.
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
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "min", minFloat, "max", maxFloat, "result", result)
	return result, nil
}

// ConcatOp implements @concat - concatenates strings.
type ConcatOp struct{}

func (o *ConcatOp) Name() string { return "@concat" }

func (o *ConcatOp) Evaluate(ctx *Context, args Args) (any, error) {
	elements, err := getElements(args, o.Name())
	if err != nil {
		return nil, err
	}

	var result string
	for i, elem := range elements {
		v, err := elem.Eval(ctx)
		if err != nil {
			return nil, fmt.Errorf("@concat[%d]: %w", i, err)
		}
		s, err := AsString(v)
		if err != nil {
			return nil, fmt.Errorf("@concat[%d]: %w", i, err)
		}
		result += s
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// AbsOp implements @abs - returns the absolute value.
type AbsOp struct{}

func (o *AbsOp) Name() string { return "@abs" }

func (o *AbsOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	// Check for int types first.
	switch v := value.(type) {
	case int:
		if v < 0 {
			v = -v
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", v)
		return int64(v), nil
	case int64:
		if v < 0 {
			v = -v
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", v)
		return v, nil
	case float64:
		if v < 0 {
			v = -v
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", v)
		return v, nil
	case float32:
		if v < 0 {
			v = -v
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", v)
		return float64(v), nil
	}

	// For other numeric types, try conversion.
	if IsInt(value) {
		i, _ := AsInt(value)
		if i < 0 {
			i = -i
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", i)
		return i, nil
	}

	f, err := AsFloat(value)
	if err != nil {
		return nil, fmt.Errorf("@abs: %w", err)
	}
	if f < 0 {
		f = -f
	}
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", f)
	return f, nil
}

// FloorOp implements @floor - rounds down to nearest integer.
type FloorOp struct{}

func (o *FloorOp) Name() string { return "@floor" }

func (o *FloorOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
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
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// CeilOp implements @ceil - rounds up to nearest integer.
type CeilOp struct{}

func (o *CeilOp) Name() string { return "@ceil" }

func (o *CeilOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
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
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// IsNilOp implements @isnil - checks if a value is nil.
type IsNilOp struct{}

func (o *IsNilOp) Name() string { return "@isnil" }

func (o *IsNilOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	result := value == nil
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

func init() {
	MustRegister("@noop", func() Operator { return &NoopOp{} })
	MustRegister("@arg", func() Operator { return &ArgOp{} })
	MustRegister("@hash", func() Operator { return &HashOp{} })
	MustRegister("@rnd", func() Operator { return &RndOp{} })
	MustRegister("@concat", func() Operator { return &ConcatOp{} })
	MustRegister("@abs", func() Operator { return &AbsOp{} })
	MustRegister("@floor", func() Operator { return &FloorOp{} })
	MustRegister("@ceil", func() Operator { return &CeilOp{} })
	MustRegister("@isnil", func() Operator { return &IsNilOp{} })
}
