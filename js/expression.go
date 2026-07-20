package js

import (
	"fmt"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/engine/datamodel"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
)

// jsRegistrant is the registrant identity under which the JS runtime binds
// script-defined expression operators in the engine registry.
const jsRegistrant = "js"

// expressionRegister implements the JS global expression.register(name, fn).
// It registers a custom expression operator backed by a JS function: when a
// compiled pipeline evaluates {"@name": [args...]}, the argument expressions
// are evaluated engine-side and the resulting plain values are passed to fn;
// the function's return value becomes the expression result.
//
// The callback runs on the VM event loop: circuit steps execute on their own
// goroutine and block until the event loop services the call. The function
// must therefore be a plain synchronous value-in/value-out transformation;
// it must not wait on circuit output, and like any expression it should be a
// pure function of its arguments (a stateful or non-deterministic callback
// breaks retraction symmetry in incremental circuits; this is not enforced).
//
// Registration is process-wide (operators live in the expression parser's
// default registry) and init-phase only: once the VM has compiled a circuit,
// the operator set is frozen. A name held by another registrant (a built-in
// or a connector-provided operator) cannot be taken over; unregister it
// first to replace it.
func (v *VM) expressionRegister(call goja.FunctionCall) (goja.Value, error) {
	if err := v.requireInitPhase("expression.register"); err != nil {
		return nil, err
	}
	name := call.Argument(0).String()
	callable, ok := goja.AssertFunction(call.Argument(1))
	if !ok {
		return nil, fmt.Errorf("expression.register: second argument must be a function")
	}

	err := dbspexpr.RegisterCallback(name, jsRegistrant, func(args []any) (any, error) {
		var result any
		err := v.runOnLoopSync(func(rt *goja.Runtime) error {
			jsArgs := make([]goja.Value, len(args))
			for i, a := range args {
				jsArgs[i] = rt.ToValue(toPlainData(a))
			}
			ret, err := callable(goja.Undefined(), jsArgs...)
			if err != nil {
				return err
			}
			if ret == nil || goja.IsUndefined(ret) || goja.IsNull(ret) {
				result = nil
				return nil
			}
			result = ret.Export()
			return nil
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	})
	if err != nil {
		return nil, fmt.Errorf("expression.register: %w", err)
	}

	v.logger.V(1).Info("registered JS expression operator", "name", name)
	return goja.Undefined(), nil
}

// expressionUnregister implements the JS global expression.unregister(name).
// It removes a JS-registered operator so future parses no longer accept it
// (compiled circuits are unaffected). Init-phase only, like registration;
// names held by other registrants (built-ins, connector operators) are
// refused; connector operators are unregistered through the connector's own
// namespace.
func (v *VM) expressionUnregister(call goja.FunctionCall) (goja.Value, error) {
	if err := v.requireInitPhase("expression.unregister"); err != nil {
		return nil, err
	}
	name := call.Argument(0).String()
	if err := dbspexpr.UnregisterCallback(name, jsRegistrant); err != nil {
		return nil, fmt.Errorf("expression.unregister: %w", err)
	}
	v.logger.V(1).Info("unregistered JS expression operator", "name", name)
	return goja.Undefined(), nil
}

// toPlainData converts an expression value into plain JSON-like data so goja
// maps it to natural JS objects: Documents become plain maps, and containers
// are copied so the callback cannot mutate engine-owned values.
func toPlainData(value any) any {
	switch t := value.(type) {
	case datamodel.Document:
		return toPlainData(t.Fields())
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = toPlainData(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = toPlainData(val)
		}
		return out
	default:
		return value
	}
}
