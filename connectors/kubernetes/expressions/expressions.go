// Package expressions provides the Kubernetes vocabulary of expression
// operators: pure, in-process Go callbacks implementing Kubernetes API
// semantics that the generic expression language has no primitives for.
//
// The package only exposes the operator table; nothing is registered as a
// side effect of linking it. Embedders (the dbsp JS runtime's
// kubernetes.expression.register, or a Go program calling the engine's
// RegisterCallback directly) bind the operators a program actually uses.
//
// Every operator is a pure function of its arguments (the contract of
// engine expression callbacks) and none requires a client, a kubeconfig, or
// a running connector runtime.
package expressions

import (
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
)

// Registrant is the identity under which the connector's operators are bound
// in the engine registry.
const Registrant = "kubernetes"

// Ops returns the operator table: name to callback. The map is freshly
// allocated on every call, so callers may not mutate shared state through it.
func Ops() map[string]dbspexpr.CallbackFunc {
	return map[string]dbspexpr.CallbackFunc{
		"@selectorMatches": selectorMatches,
		"@rego":            regoEval,
	}
}

// selectorMatches implements {"@selectorMatches": [selector, labels]}: a
// Kubernetes LabelSelector (matchLabels + matchExpressions, standard
// semantics: an empty selector matches everything) evaluated against a label
// map. Following the Kubernetes convention for selector handling, a selector
// or label map that fails validation matches nothing (fail closed) rather
// than erroring: an expression error would abort the whole circuit step, and
// selectors from watched resources have already passed API-server
// validation.
func selectorMatches(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("@selectorMatches expects [selector, labels], got %d arguments", len(args))
	}

	sel, err := toLabelSelector(args[0])
	if err != nil {
		return false, nil //nolint:nilerr // fail closed on invalid selectors
	}
	compiled, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return false, nil //nolint:nilerr // fail closed on invalid selectors
	}

	lbl, err := toLabelSet(args[1])
	if err != nil {
		return false, nil //nolint:nilerr // fail closed on invalid label maps
	}
	return compiled.Matches(lbl), nil
}

// toLabelSelector converts an expression value (a plain map following the
// LabelSelector JSON shape, or nil for "no selector") into a typed selector.
// A nil selector converts to a match-everything selector, mirroring the
// pipeline convention of defaulting an absent selector to {}.
func toLabelSelector(value any) (*metav1.LabelSelector, error) {
	if value == nil {
		return &metav1.LabelSelector{}, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	sel := &metav1.LabelSelector{}
	if err := json.Unmarshal(raw, sel); err != nil {
		return nil, err
	}
	return sel, nil
}

// toLabelSet converts an expression value into a label set via its JSON
// form (values may be plain maps or documents); label values must be
// strings, as in the Kubernetes API.
func toLabelSet(value any) (labels.Set, error) {
	if value == nil {
		return labels.Set{}, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	set := labels.Set{}
	if err := json.Unmarshal(raw, &set); err != nil {
		return nil, fmt.Errorf("labels must be a string map: %w", err)
	}
	return set, nil
}
