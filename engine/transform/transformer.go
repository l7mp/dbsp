package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
)

// TransformerType identifies a transformer.
type TransformerType string

const (
	Incrementalizer TransformerType = "Incrementalizer"
	Rewriter        TransformerType = "Rewriter"
	Reconciler      TransformerType = "Reconciler"
)

// Transformer transforms one circuit into another.
//
// All implementations use copy-and-update semantics: they clone the input and
// never mutate it.
type Transformer interface {
	Name() TransformerType
	Transform(c *circuit.Circuit) (*circuit.Circuit, error)
}

// New creates a transformer by type.
func New(typ TransformerType, args ...any) (Transformer, error) {
	switch typ {
	case Incrementalizer:
		return NewIncrementalizer(), nil
	case Rewriter:
		rules, err := parseRewriterArgs(args)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", Rewriter, err)
		}
		return NewRewriter(rules...), nil
	case Reconciler:
		pairs, err := parseReconcilerArgs(args)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", Reconciler, err)
		}
		return NewReconciler(pairs...), nil
	default:
		return nil, fmt.Errorf("unknown transformer: %q", typ)
	}
}

func parseRewriterArgs(args []any) ([]RewriteRule, error) {
	if len(args) == 0 {
		return DefaultRules(), nil
	}

	if len(args) == 1 {
		switch v := args[0].(type) {
		case string:
			switch v {
			case "Pre":
				return PreRules(), nil
			case "Post":
				return PostRules(), nil
			case "Default":
				return DefaultRules(), nil
			default:
				return nil, fmt.Errorf("unknown ruleset %q", v)
			}
		case RewriteRule:
			return []RewriteRule{v}, nil
		}
	}

	rules := make([]RewriteRule, 0, len(args))
	for i, arg := range args {
		rule, ok := arg.(RewriteRule)
		if !ok {
			return nil, fmt.Errorf("arg %d must be RewriteRule, got %T", i, arg)
		}
		rules = append(rules, rule)
	}

	if len(rules) == 0 {
		return DefaultRules(), nil
	}

	return rules, nil
}

func parseReconcilerArgs(args []any) ([]ReconcilerPair, error) {
	if len(args) == 0 {
		return nil, nil
	}

	if len(args) != 1 {
		return nil, fmt.Errorf("expected zero args or one []ReconcilerPair argument")
	}

	pairs, ok := args[0].([]ReconcilerPair)
	if !ok {
		return nil, fmt.Errorf("expected []ReconcilerPair argument, got %T", args[0])
	}

	return pairs, nil
}
