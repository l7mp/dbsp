package dbsp

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/l7mp/dbsp/expression"
)

// regexpExpr implements @regexp - regex pattern matching.
type regexpExpr struct{ binaryOp }

func (e *regexpExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	patternVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@regexp: pattern: %w", err)
	}
	pattern, err := AsString(patternVal)
	if err != nil {
		return nil, fmt.Errorf("@regexp: pattern must be string: %w", err)
	}

	strVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@regexp: string: %w", err)
	}
	str, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@regexp: string must be string: %w", err)
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("@regexp: invalid pattern: %w", err)
	}

	result := re.MatchString(str)
	ctx.Logger().V(8).Info("eval", "op", "@regexp", "pattern", pattern, "result", result)
	return result, nil
}

// upperExpr implements @upper - converts string to uppercase.
type upperExpr struct{ unaryOp }

func (e *upperExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	s, err := AsString(value)
	if err != nil {
		return nil, fmt.Errorf("@upper: %w", err)
	}
	result := strings.ToUpper(s)
	ctx.Logger().V(8).Info("eval", "op", "@upper", "result", result)
	return result, nil
}

// lowerExpr implements @lower - converts string to lowercase.
type lowerExpr struct{ unaryOp }

func (e *lowerExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	s, err := AsString(value)
	if err != nil {
		return nil, fmt.Errorf("@lower: %w", err)
	}
	result := strings.ToLower(s)
	ctx.Logger().V(8).Info("eval", "op", "@lower", "result", result)
	return result, nil
}

// trimExpr implements @trim - removes leading and trailing whitespace.
type trimExpr struct{ unaryOp }

func (e *trimExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	value, err := e.operand.Evaluate(ctx)
	if err != nil {
		return nil, err
	}
	s, err := AsString(value)
	if err != nil {
		return nil, fmt.Errorf("@trim: %w", err)
	}
	result := strings.TrimSpace(s)
	ctx.Logger().V(8).Info("eval", "op", "@trim", "result", result)
	return result, nil
}

// substringExpr implements @substring - extracts a substring.
// Start is 1-based (SQL style). If start is negative, counts from end.
type substringExpr struct{ variadicOp }

func (e *substringExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if len(e.args) != 2 && len(e.args) != 3 {
		return nil, fmt.Errorf("@substring: expected [string, start] or [string, start, length] arguments")
	}

	strVal, err := e.args[0].Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@substring: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@substring: first argument must be string: %w", err)
	}

	startVal, err := e.args[1].Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@substring: start: %w", err)
	}
	start, err := AsInt(startVal)
	if err != nil {
		return nil, fmt.Errorf("@substring: start must be integer: %w", err)
	}

	if start > 0 {
		start--
	} else if start < 0 {
		start = int64(len(s)) + start
	}

	if start < 0 {
		start = 0
	}
	if start > int64(len(s)) {
		start = int64(len(s))
	}

	var result string
	if len(e.args) == 3 {
		lengthVal, err := e.args[2].Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@substring: length: %w", err)
		}
		length, err := AsInt(lengthVal)
		if err != nil {
			return nil, fmt.Errorf("@substring: length must be integer: %w", err)
		}
		if length < 0 {
			length = 0
		}
		end := start + length
		if end > int64(len(s)) {
			end = int64(len(s))
		}
		result = s[start:end]
	} else {
		result = s[start:]
	}

	ctx.Logger().V(8).Info("eval", "op", "@substring", "result", result)
	return result, nil
}

// replaceExpr implements @replace - replaces occurrences of a substring.
type replaceExpr struct{ variadicOp }

func (e *replaceExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	if len(e.args) != 3 && len(e.args) != 4 {
		return nil, fmt.Errorf("@replace: expected [string, old, new] or [string, old, new, count] arguments")
	}

	strVal, err := e.args[0].Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@replace: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@replace: first argument must be string: %w", err)
	}

	oldVal, err := e.args[1].Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@replace: old: %w", err)
	}
	old, err := AsString(oldVal)
	if err != nil {
		return nil, fmt.Errorf("@replace: old must be string: %w", err)
	}

	newVal, err := e.args[2].Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@replace: new: %w", err)
	}
	newStr, err := AsString(newVal)
	if err != nil {
		return nil, fmt.Errorf("@replace: new must be string: %w", err)
	}

	count := -1
	if len(e.args) == 4 {
		countVal, err := e.args[3].Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("@replace: count: %w", err)
		}
		c, err := AsInt(countVal)
		if err != nil {
			return nil, fmt.Errorf("@replace: count must be integer: %w", err)
		}
		count = int(c)
	}

	result := strings.Replace(s, old, newStr, count)
	ctx.Logger().V(8).Info("eval", "op", "@replace", "result", result)
	return result, nil
}

// splitExpr implements @split - splits a string into a list.
type splitExpr struct{ binaryOp }

func (e *splitExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	strVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@split: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@split: first argument must be string: %w", err)
	}

	sepVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@split: separator: %w", err)
	}
	sep, err := AsString(sepVal)
	if err != nil {
		return nil, fmt.Errorf("@split: separator must be string: %w", err)
	}

	parts := strings.Split(s, sep)
	result := make([]any, len(parts))
	for i, p := range parts {
		result[i] = p
	}

	ctx.Logger().V(8).Info("eval", "op", "@split", "result", result)
	return result, nil
}

// joinExpr implements @join - joins a list into a string.
type joinExpr struct{ binaryOp }

func (e *joinExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	listVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@join: list: %w", err)
	}
	list, err := AsList(listVal)
	if err != nil {
		return nil, fmt.Errorf("@join: first argument must be list: %w", err)
	}

	sepVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@join: separator: %w", err)
	}
	sep, err := AsString(sepVal)
	if err != nil {
		return nil, fmt.Errorf("@join: separator must be string: %w", err)
	}

	strs := make([]string, len(list))
	for i, item := range list {
		s, err := AsString(item)
		if err != nil {
			return nil, fmt.Errorf("@join[%d]: %w", i, err)
		}
		strs[i] = s
	}

	result := strings.Join(strs, sep)
	ctx.Logger().V(8).Info("eval", "op", "@join", "result", result)
	return result, nil
}

// startsWithExpr implements @startswith.
type startsWithExpr struct{ binaryOp }

func (e *startsWithExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	strVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@startswith: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@startswith: first argument must be string: %w", err)
	}

	prefixVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@startswith: prefix: %w", err)
	}
	prefix, err := AsString(prefixVal)
	if err != nil {
		return nil, fmt.Errorf("@startswith: prefix must be string: %w", err)
	}

	result := strings.HasPrefix(s, prefix)
	ctx.Logger().V(8).Info("eval", "op", "@startswith", "result", result)
	return result, nil
}

// endsWithExpr implements @endswith.
type endsWithExpr struct{ binaryOp }

func (e *endsWithExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	strVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@endswith: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@endswith: first argument must be string: %w", err)
	}

	suffixVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@endswith: suffix: %w", err)
	}
	suffix, err := AsString(suffixVal)
	if err != nil {
		return nil, fmt.Errorf("@endswith: suffix must be string: %w", err)
	}

	result := strings.HasSuffix(s, suffix)
	ctx.Logger().V(8).Info("eval", "op", "@endswith", "result", result)
	return result, nil
}

// containsExpr implements @contains.
type containsExpr struct{ binaryOp }

func (e *containsExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	strVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@contains: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@contains: first argument must be string: %w", err)
	}

	subVal, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@contains: substring: %w", err)
	}
	sub, err := AsString(subVal)
	if err != nil {
		return nil, fmt.Errorf("@contains: substring must be string: %w", err)
	}

	result := strings.Contains(s, sub)
	ctx.Logger().V(8).Info("eval", "op", "@contains", "result", result)
	return result, nil
}

func init() {
	registerBinaryStrOp := func(name string, factory func(a, b Expression) Expression) {
		MustRegister(name, func(args any) (Expression, error) {
			left, right, err := asBinaryExprs(args, name)
			if err != nil {
				return nil, err
			}
			return factory(left, right), nil
		})
	}

	registerBinaryStrOp("@regexp", func(a, b Expression) Expression {
		return &regexpExpr{binaryOp{"@regexp", a, b}}
	})
	registerBinaryStrOp("@split", func(a, b Expression) Expression {
		return &splitExpr{binaryOp{"@split", a, b}}
	})
	registerBinaryStrOp("@join", func(a, b Expression) Expression {
		return &joinExpr{binaryOp{"@join", a, b}}
	})
	registerBinaryStrOp("@startswith", func(a, b Expression) Expression {
		return &startsWithExpr{binaryOp{"@startswith", a, b}}
	})
	registerBinaryStrOp("@endswith", func(a, b Expression) Expression {
		return &endsWithExpr{binaryOp{"@endswith", a, b}}
	})
	registerBinaryStrOp("@contains", func(a, b Expression) Expression {
		return &containsExpr{binaryOp{"@contains", a, b}}
	})

	registerUnaryStrOp := func(name string, factory func(operand Expression) Expression) {
		MustRegister(name, func(args any) (Expression, error) {
			operand, err := asUnaryExprOrLiteral(args)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			return factory(operand), nil
		})
	}

	registerUnaryStrOp("@upper", func(o Expression) Expression { return &upperExpr{unaryOp{"@upper", o}} })
	registerUnaryStrOp("@lower", func(o Expression) Expression { return &lowerExpr{unaryOp{"@lower", o}} })
	registerUnaryStrOp("@trim", func(o Expression) Expression { return &trimExpr{unaryOp{"@trim", o}} })

	MustRegister("@substring", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || (len(list) != 2 && len(list) != 3) {
			return nil, fmt.Errorf("@substring: expected [string, start] or [string, start, length] arguments")
		}
		return &substringExpr{variadicOp{"@substring", list}}, nil
	})
	MustRegister("@replace", func(args any) (Expression, error) {
		list, ok := args.([]Expression)
		if !ok || (len(list) != 3 && len(list) != 4) {
			return nil, fmt.Errorf("@replace: expected [string, old, new] or [string, old, new, count] arguments")
		}
		return &replaceExpr{variadicOp{"@replace", list}}, nil
	})
}
