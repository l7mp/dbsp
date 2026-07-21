package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

// TransformerType identifies a transformer.
type TransformerType string

const (
	Incrementalizer TransformerType = "Incrementalizer"
	Rewriter        TransformerType = "Rewriter"
	Reconciler      TransformerType = "Reconciler"
	Regularizer     TransformerType = "Regularizer"
	Optimizer       TransformerType = "Optimizer"
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
	case Regularizer:
		return NewRegularizer(), nil
	case Optimizer:
		opts, err := parseOptimizerArgs(args)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", Optimizer, err)
		}
		return NewOptimizer(opts), nil
	default:
		return nil, fmt.Errorf("unknown transformer: %q", typ)
	}
}

type OptimizerOptions struct {
	RewriterRules   []RewriteRule
	ReconcilerPairs []ReconcilerPair
}

type optimizer struct {
	opts OptimizerOptions
}

// NewOptimizer creates a meta-transform that applies transforms in canonical
// order.
func NewOptimizer(opts OptimizerOptions) Transformer {
	return &optimizer{opts: opts}
}

func (t *optimizer) Name() TransformerType { return Optimizer }

func (t *optimizer) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	if c == nil {
		return nil, fmt.Errorf("optimizer: nil circuit")
	}

	// Runtime guardrails: Optimizer is meant for snapshot circuits.
	for _, n := range c.Nodes() {
		if n.Kind() == operator.KindDifferentiate || n.Kind() == operator.KindIntegrate {
			return nil, fmt.Errorf("optimizer: expected snapshot circuit, found incremental node %q (%s)", n.ID, n.Kind())
		}
	}

	current := c
	var err error

	rules := t.opts.RewriterRules
	if len(rules) == 0 {
		rules = DefaultRules()
	}
	current, err = NewRewriter(rules...).Transform(current)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Rewriter, err)
	}

	current, err = NewReconciler(t.opts.ReconcilerPairs...).Transform(current)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Reconciler, err)
	}

	current, err = NewRegularizer().Transform(current)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Regularizer, err)
	}

	current, err = NewIncrementalizer().Transform(current)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Incrementalizer, err)
	}

	return current, nil
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

func parseOptimizerArgs(args []any) (OptimizerOptions, error) {
	if len(args) == 0 {
		return OptimizerOptions{}, nil
	}
	if len(args) != 1 {
		return OptimizerOptions{}, fmt.Errorf("expected zero args or one OptimizerOptions argument")
	}

	opts, ok := args[0].(OptimizerOptions)
	if ok {
		return opts, nil
	}

	optsPtr, ok := args[0].(*OptimizerOptions)
	if ok && optsPtr != nil {
		return *optsPtr, nil
	}

	return OptimizerOptions{}, fmt.Errorf("expected OptimizerOptions argument, got %T", args[0])
}
