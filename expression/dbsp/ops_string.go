package dbsp

import (
	"fmt"
	"regexp"
	"strings"
)

// RegexpOp implements @regexp - regex pattern matching.
// Arguments: [pattern, string] - returns true if string matches pattern.
type RegexpOp struct{}

func (o *RegexpOp) Name() string { return "@regexp" }

func (o *RegexpOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@regexp: expected [pattern, string] arguments")
	}

	patternVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@regexp: pattern: %w", err)
	}
	pattern, err := AsString(patternVal)
	if err != nil {
		return nil, fmt.Errorf("@regexp: pattern must be string: %w", err)
	}

	strVal, err := listArgs.Elements[1].Eval(ctx)
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
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "pattern", pattern, "result", result)
	return result, nil
}

// UpperOp implements @upper - converts string to uppercase.
type UpperOp struct{}

func (o *UpperOp) Name() string { return "@upper" }

func (o *UpperOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	s, err := AsString(value)
	if err != nil {
		return nil, fmt.Errorf("@upper: %w", err)
	}

	result := strings.ToUpper(s)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// LowerOp implements @lower - converts string to lowercase.
type LowerOp struct{}

func (o *LowerOp) Name() string { return "@lower" }

func (o *LowerOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	s, err := AsString(value)
	if err != nil {
		return nil, fmt.Errorf("@lower: %w", err)
	}

	result := strings.ToLower(s)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// TrimOp implements @trim - removes leading and trailing whitespace.
type TrimOp struct{}

func (o *TrimOp) Name() string { return "@trim" }

func (o *TrimOp) Evaluate(ctx *Context, args Args) (any, error) {
	value, err := evaluateSingleArg(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	s, err := AsString(value)
	if err != nil {
		return nil, fmt.Errorf("@trim: %w", err)
	}

	result := strings.TrimSpace(s)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// SubstringOp implements @substring - extracts a substring.
// Arguments: [string, start] or [string, start, length]
// Start is 1-based (SQL style). If start is negative, counts from end.
type SubstringOp struct{}

func (o *SubstringOp) Name() string { return "@substring" }

func (o *SubstringOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || (len(listArgs.Elements) != 2 && len(listArgs.Elements) != 3) {
		return nil, fmt.Errorf("@substring: expected [string, start] or [string, start, length] arguments")
	}

	strVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@substring: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@substring: first argument must be string: %w", err)
	}

	startVal, err := listArgs.Elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@substring: start: %w", err)
	}
	start, err := AsInt(startVal)
	if err != nil {
		return nil, fmt.Errorf("@substring: start must be integer: %w", err)
	}

	// Convert 1-based to 0-based index.
	if start > 0 {
		start--
	} else if start < 0 {
		// Negative start counts from end.
		start = int64(len(s)) + start
	}

	if start < 0 {
		start = 0
	}
	if start > int64(len(s)) {
		start = int64(len(s))
	}

	var result string
	if len(listArgs.Elements) == 3 {
		lengthVal, err := listArgs.Elements[2].Eval(ctx)
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

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// ReplaceOp implements @replace - replaces occurrences of a substring.
// Arguments: [string, old, new] or [string, old, new, count]
// If count is omitted or -1, replaces all occurrences.
type ReplaceOp struct{}

func (o *ReplaceOp) Name() string { return "@replace" }

func (o *ReplaceOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || (len(listArgs.Elements) != 3 && len(listArgs.Elements) != 4) {
		return nil, fmt.Errorf("@replace: expected [string, old, new] or [string, old, new, count] arguments")
	}

	strVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@replace: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@replace: first argument must be string: %w", err)
	}

	oldVal, err := listArgs.Elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@replace: old: %w", err)
	}
	old, err := AsString(oldVal)
	if err != nil {
		return nil, fmt.Errorf("@replace: old must be string: %w", err)
	}

	newVal, err := listArgs.Elements[2].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@replace: new: %w", err)
	}
	newStr, err := AsString(newVal)
	if err != nil {
		return nil, fmt.Errorf("@replace: new must be string: %w", err)
	}

	count := -1 // Replace all by default.
	if len(listArgs.Elements) == 4 {
		countVal, err := listArgs.Elements[3].Eval(ctx)
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
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// SplitOp implements @split - splits a string into a list.
// Arguments: [string, separator]
type SplitOp struct{}

func (o *SplitOp) Name() string { return "@split" }

func (o *SplitOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@split: expected [string, separator] arguments")
	}

	strVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@split: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@split: first argument must be string: %w", err)
	}

	sepVal, err := listArgs.Elements[1].Eval(ctx)
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

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// JoinOp implements @join - joins a list into a string.
// Arguments: [list, separator]
type JoinOp struct{}

func (o *JoinOp) Name() string { return "@join" }

func (o *JoinOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@join: expected [list, separator] arguments")
	}

	listVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@join: list: %w", err)
	}
	list, err := AsList(listVal)
	if err != nil {
		return nil, fmt.Errorf("@join: first argument must be list: %w", err)
	}

	sepVal, err := listArgs.Elements[1].Eval(ctx)
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
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// StartsWithOp implements @startswith - checks if string starts with prefix.
type StartsWithOp struct{}

func (o *StartsWithOp) Name() string { return "@startswith" }

func (o *StartsWithOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@startswith: expected [string, prefix] arguments")
	}

	strVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@startswith: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@startswith: first argument must be string: %w", err)
	}

	prefixVal, err := listArgs.Elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@startswith: prefix: %w", err)
	}
	prefix, err := AsString(prefixVal)
	if err != nil {
		return nil, fmt.Errorf("@startswith: prefix must be string: %w", err)
	}

	result := strings.HasPrefix(s, prefix)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// EndsWithOp implements @endswith - checks if string ends with suffix.
type EndsWithOp struct{}

func (o *EndsWithOp) Name() string { return "@endswith" }

func (o *EndsWithOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@endswith: expected [string, suffix] arguments")
	}

	strVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@endswith: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@endswith: first argument must be string: %w", err)
	}

	suffixVal, err := listArgs.Elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@endswith: suffix: %w", err)
	}
	suffix, err := AsString(suffixVal)
	if err != nil {
		return nil, fmt.Errorf("@endswith: suffix must be string: %w", err)
	}

	result := strings.HasSuffix(s, suffix)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

// ContainsOp implements @contains - checks if string contains substring.
type ContainsOp struct{}

func (o *ContainsOp) Name() string { return "@contains" }

func (o *ContainsOp) Evaluate(ctx *Context, args Args) (any, error) {
	listArgs, ok := args.(ListArgs)
	if !ok || len(listArgs.Elements) != 2 {
		return nil, fmt.Errorf("@contains: expected [string, substring] arguments")
	}

	strVal, err := listArgs.Elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@contains: string: %w", err)
	}
	s, err := AsString(strVal)
	if err != nil {
		return nil, fmt.Errorf("@contains: first argument must be string: %w", err)
	}

	subVal, err := listArgs.Elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("@contains: substring: %w", err)
	}
	sub, err := AsString(subVal)
	if err != nil {
		return nil, fmt.Errorf("@contains: substring must be string: %w", err)
	}

	result := strings.Contains(s, sub)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

func init() {
	MustRegister("@regexp", func() Operator { return &RegexpOp{} })
	MustRegister("@upper", func() Operator { return &UpperOp{} })
	MustRegister("@lower", func() Operator { return &LowerOp{} })
	MustRegister("@trim", func() Operator { return &TrimOp{} })
	MustRegister("@substring", func() Operator { return &SubstringOp{} })
	MustRegister("@replace", func() Operator { return &ReplaceOp{} })
	MustRegister("@split", func() Operator { return &SplitOp{} })
	MustRegister("@join", func() Operator { return &JoinOp{} })
	MustRegister("@startswith", func() Operator { return &StartsWithOp{} })
	MustRegister("@endswith", func() Operator { return &EndsWithOp{} })
	MustRegister("@contains", func() Operator { return &ContainsOp{} })
}
