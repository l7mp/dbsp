package dbsp

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/expression"
)

// CallbackFunc is a host-provided function backing a callback expression. It
// receives the evaluated argument values and returns the expression result.
// The values follow the standard expression data model (nil, bool, int64,
// float64, string, []any, map[string]any, datamodel.Document).
//
// Callbacks run inside operator evaluation, so they are subject to the same
// expectations as built-in operators: they should be pure functions of their
// arguments. A callback that is non-deterministic or keeps state across calls
// breaks retraction symmetry in incremental circuits, exactly like @now in a
// group key. This is not enforced.
type CallbackFunc func(args []any) (any, error)

// callbackExpr is an operator backed by a host-registered callback: it
// evaluates its argument expressions and passes the values to the callback.
type callbackExpr struct {
	name string
	args []Expression
	fn   CallbackFunc
}

func (e *callbackExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	vals := make([]any, len(e.args))
	for i, arg := range e.args {
		v, err := arg.Evaluate(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s: argument %d: %w", e.name, i, err)
		}
		vals[i] = v
	}
	result, err := e.fn(vals)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", e.name, err)
	}
	ctx.Logger().V(8).Info("eval", "op", e.name, "result", result)
	return result, nil
}

func (e *callbackExpr) String() string {
	return fmt.Sprintf("%s(%d args)", e.name, len(e.args))
}

func (e *callbackExpr) MarshalJSON() ([]byte, error) {
	return marshalVariadicOp(e.name, e.args)
}

func (e *callbackExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// RegisterCallback registers an operator backed by a host-provided callback
// on behalf of a registrant (the component owning the callback: the JS
// runtime, a connector, an embedding application). The operator accepts any
// argument form; the arguments are evaluated and the resulting values are
// passed to the callback.
//
// The registrant is the collision authority: a built-in name can never be
// taken over, a callback name held by a different registrant is an error
// (naming the holder), and a registrant may re-register its own names freely
// (embedders re-initialize across runtime instances).
//
// The callback's execution model is the registrant's business: an in-process
// Go function and a function that marshals the call to another goroutine
// (the JS runtime services callbacks on its event loop) register the same
// way; the engine only requires that the call is synchronous from its point
// of view and a pure function of its arguments.
func (r *Registry) RegisterCallback(name, registrant string, fn CallbackFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(name) == 0 || name[0] != '@' {
		return fmt.Errorf("operator name must start with @: %q", name)
	}
	if registrant == "" {
		return fmt.Errorf("operator %q: registrant must not be empty", name)
	}
	if _, exists := r.operators[name]; exists {
		holder, isCallback := r.callbacks[name]
		if !isCallback {
			return fmt.Errorf("operator %q is a built-in and cannot be replaced", name)
		}
		if holder != registrant {
			return fmt.Errorf("operator %q is already registered by %q", name, holder)
		}
	}
	r.operators[name] = func(args any) (Expression, error) {
		return &callbackExpr{name: name, args: callbackArgs(args), fn: fn}, nil
	}
	r.callbacks[name] = registrant
	return nil
}

// UnregisterCallback removes a callback-backed operator. Only the registrant
// holding the name may remove it, and built-in operators cannot be removed.
// Already-compiled expressions hold their callback directly and are
// unaffected; unregistration only changes what future parses accept.
func (r *Registry) UnregisterCallback(name, registrant string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	holder, isCallback := r.callbacks[name]
	if !isCallback {
		if _, exists := r.operators[name]; exists {
			return fmt.Errorf("operator %q is a built-in and cannot be unregistered", name)
		}
		return fmt.Errorf("operator %q is not registered", name)
	}
	if holder != registrant {
		return fmt.Errorf("operator %q is registered by %q", name, holder)
	}
	delete(r.operators, name)
	delete(r.callbacks, name)
	return nil
}

// CallbackRegistrant reports the registrant holding a callback-backed
// operator name, or false when the name is unbound or a built-in.
func (r *Registry) CallbackRegistrant(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	holder, ok := r.callbacks[name]
	return holder, ok
}

// RegisterCallback registers a callback operator with the default registry.
func RegisterCallback(name, registrant string, fn CallbackFunc) error {
	return DefaultRegistry.RegisterCallback(name, registrant, fn)
}

// UnregisterCallback removes a callback operator from the default registry.
func UnregisterCallback(name, registrant string) error {
	return DefaultRegistry.UnregisterCallback(name, registrant)
}

// CallbackRegistrant reports the holder of a callback operator name in the
// default registry.
func CallbackRegistrant(name string) (string, bool) {
	return DefaultRegistry.CallbackRegistrant(name)
}

// callbackArgs normalizes parsed factory args into an argument list.
func callbackArgs(args any) []Expression {
	switch a := args.(type) {
	case nil:
		return nil
	case Expression:
		return []Expression{a}
	case []Expression:
		return a
	case map[string]Expression:
		return []Expression{&dictExpr{entries: a}}
	default:
		// Scalar literal (bool/int64/float64/string).
		return []Expression{&literalExpr{value: a}}
	}
}
