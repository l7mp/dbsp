package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/circuit"
)

// TransformerType identifies a transformer.
type TransformerType string

const (
	Incrementalizer TransformerType = "Incrementalizer"
	Reconciler      TransformerType = "Reconciler"
	SmithPredictor  TransformerType = "SmithPredictor"
	Distincter      TransformerType = "Distincter"

	// Rewriter is the internal algebraic rewrite pass. It is not
	// registered with New and carries no canonical rank: the
	// Incrementalizer applies it automatically, and Go embedders can run
	// NewRewriter directly.
	Rewriter TransformerType = "Rewriter"
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
	case Reconciler:
		pairs, err := parseReconcilerArgs(args)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", Reconciler, err)
		}
		return NewReconciler(pairs...), nil
	case SmithPredictor:
		k, pairs, err := parseSmithArgs(args)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", SmithPredictor, err)
		}
		return NewSmithPredictor(k, pairs...), nil
	case Distincter:
		return NewDistincter(), nil
	default:
		return nil, fmt.Errorf("unknown transformer: %q", typ)
	}
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

// Spec pairs a transformer type with its constructor arguments, as accepted
// by New.
type Spec struct {
	Type TransformerType
	Args []any
}

// canonicalRank orders the transforms along the incrementalization boundary:
// the control-loop transforms first (Reconciler, SmithPredictor), then
// Distincter, then the Incrementalizer. Equal ranks preserve the caller's
// relative order.
var canonicalRank = map[TransformerType]int{
	Reconciler:      20,
	SmithPredictor:  20,
	Distincter:      30,
	Incrementalizer: 40,
}

// Chain is a meta-transformer that applies a set of transforms in canonical
// order, regardless of the order they were specified in. Callers list what
// they want; the chain knows which side of the incrementalization boundary
// each transform belongs to.
type Chain struct {
	specs []Spec
}

// NewChain validates a set of transform specs and returns a Chain that
// applies them in canonical order. It rejects duplicates and unknown
// types.
func NewChain(specs ...Spec) (*Chain, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("chain: no transforms given")
	}

	seen := map[TransformerType]bool{}
	for _, s := range specs {
		if _, ok := canonicalRank[s.Type]; !ok {
			return nil, fmt.Errorf("chain: unknown transformer %q", s.Type)
		}
		if seen[s.Type] {
			return nil, fmt.Errorf("chain: transformer %q listed twice", s.Type)
		}
		seen[s.Type] = true
	}
	// Stable sort by rank: equal ranks keep the given order.
	sorted := append([]Spec(nil), specs...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && canonicalRank[sorted[j].Type] < canonicalRank[sorted[j-1].Type]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	return &Chain{specs: sorted}, nil
}

// Specs returns the chain's transforms in the order they will be applied.
func (t *Chain) Specs() []Spec {
	return append([]Spec(nil), t.specs...)
}

// Name implements Transformer.
func (t *Chain) Name() TransformerType { return "Chain" }

// Transform implements Transformer: it applies the chain's transforms in
// canonical order, threading the circuit through.
func (t *Chain) Transform(c *circuit.Circuit) (*circuit.Circuit, error) {
	current := c
	for _, s := range t.specs {
		tr, err := New(s.Type, s.Args...)
		if err != nil {
			return nil, fmt.Errorf("chain: %w", err)
		}
		current, err = tr.Transform(current)
		if err != nil {
			return nil, fmt.Errorf("chain: %s: %w", s.Type, err)
		}
	}
	return current, nil
}

func parseSmithArgs(args []any) (int, []ReconcilerPair, error) {
	var pairs []ReconcilerPair
	k := 0
	for i, arg := range args {
		switch v := arg.(type) {
		case int:
			k = v
		case []ReconcilerPair:
			pairs = v
		default:
			return 0, nil, fmt.Errorf("arg %d: expected int or []ReconcilerPair, got %T", i, arg)
		}
	}
	return k, pairs, nil
}
