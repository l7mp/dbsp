package js

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dop251/goja"

	k8sexpr "github.com/l7mp/dbsp/connectors/kubernetes/expressions"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
)

// The kubernetes.expression namespace binds the Kubernetes connector's
// Go-implemented expression operators into the language. Nothing is
// registered implicitly: a script names the operators it needs, in the init
// phase (before the first circuit is compiled). The operators are pure and
// independent of the connector runtime: no kubeconfig or cluster is needed.
//
//	kubernetes.expression.register("@selectorMatches", ...)
//	kubernetes.expression.unregister("@selectorMatches", ...)
//	kubernetes.expression.list()  // the names the connector offers

// k8sExpressionRegister implements kubernetes.expression.register(...names).
func (v *VM) k8sExpressionRegister(call goja.FunctionCall) (goja.Value, error) {
	if err := v.requireInitPhase("kubernetes.expression.register"); err != nil {
		return nil, err
	}
	names, err := expressionNameArgs("kubernetes.expression.register", call)
	if err != nil {
		return nil, err
	}
	ops := k8sexpr.Ops()
	for _, name := range names {
		fn, ok := ops[name]
		if !ok {
			return nil, fmt.Errorf("kubernetes.expression.register: unknown operator %q (available: %s)",
				name, strings.Join(sortedOpNames(ops), ", "))
		}
		if err := dbspexpr.RegisterCallback(name, k8sexpr.Registrant, fn); err != nil {
			return nil, fmt.Errorf("kubernetes.expression.register: %w", err)
		}
		v.logger.V(1).Info("registered kubernetes expression operator", "name", name)
	}
	return goja.Undefined(), nil
}

// k8sExpressionUnregister implements kubernetes.expression.unregister(...names).
func (v *VM) k8sExpressionUnregister(call goja.FunctionCall) (goja.Value, error) {
	if err := v.requireInitPhase("kubernetes.expression.unregister"); err != nil {
		return nil, err
	}
	names, err := expressionNameArgs("kubernetes.expression.unregister", call)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if err := dbspexpr.UnregisterCallback(name, k8sexpr.Registrant); err != nil {
			return nil, fmt.Errorf("kubernetes.expression.unregister: %w", err)
		}
		v.logger.V(1).Info("unregistered kubernetes expression operator", "name", name)
	}
	return goja.Undefined(), nil
}

// k8sExpressionList implements kubernetes.expression.list(): the sorted
// names of the operators the connector offers.
func (v *VM) k8sExpressionList(_ goja.FunctionCall) (goja.Value, error) {
	return v.rt.ToValue(sortedOpNames(k8sexpr.Ops())), nil
}

// expressionNameArgs extracts the variadic operator-name arguments.
func expressionNameArgs(what string, call goja.FunctionCall) ([]string, error) {
	if len(call.Arguments) == 0 {
		return nil, fmt.Errorf("%s: at least one operator name is required", what)
	}
	names := make([]string, len(call.Arguments))
	for i, arg := range call.Arguments {
		names[i] = arg.String()
	}
	return names, nil
}

func sortedOpNames(ops map[string]dbspexpr.CallbackFunc) []string {
	names := make([]string, 0, len(ops))
	for name := range ops {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
