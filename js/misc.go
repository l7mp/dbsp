package js

import (
	"fmt"

	"github.com/dop251/goja"

	misc "github.com/l7mp/dbsp/connectors/misc"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// The misc namespace binds the misc connector's trigger producers:
// `misc.tick(topic, {kind, name, namespace, period})` emits a trigger
// document every period, `misc.init(topic, {kind, name, namespace})` emits
// one at startup. The options object is the connector's TriggerSpec wire
// form; the connector owns its validation.

func (v *VM) newMiscNamespace() (*goja.Object, error) {
	obj := v.rt.NewObject()
	if err := obj.Set("tick", v.wrap(v.miscTick)); err != nil {
		return nil, err
	}
	if err := obj.Set("init", v.wrap(v.miscInit)); err != nil {
		return nil, err
	}
	if err := obj.Set("toJSON", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(map[string]any{
			"kind": "misc",
			"apis": []string{"tick", "init"},
		}), nil
	})); err != nil {
		return nil, err
	}
	return obj, nil
}

func (v *VM) miscTick(call goja.FunctionCall) (goja.Value, error) {
	return v.installMiscTrigger("tick", call)
}

func (v *VM) miscInit(call goja.FunctionCall) (goja.Value, error) {
	return v.installMiscTrigger("init", call)
}

func (v *VM) installMiscTrigger(verb string, call goja.FunctionCall) (goja.Value, error) {
	kind := "misc." + verb
	topic, ok := call.Argument(0).Export().(string)
	if !ok || topic == "" {
		return nil, fmt.Errorf("%s(topic, opts): expected non-empty string topic, got %s", kind, describeCall(call))
	}
	var spec misc.TriggerSpec
	if err := decodeOptionValue(call.Argument(1), &spec); err != nil {
		return nil, fmt.Errorf("%s options: %w", kind, err)
	}
	deps := misc.Deps{Runtime: v.runtime, Logger: v.logger}

	var (
		p   dbspruntime.Runnable
		err error
	)
	if verb == "tick" {
		p, err = misc.NewTick(topic, spec, deps)
	} else {
		p, err = misc.NewInit(topic, spec, deps)
	}
	if err != nil {
		return nil, err
	}
	if err := v.runtime.Add(p); err != nil {
		return nil, fmt.Errorf("%s: register producer: %w", kind, err)
	}
	return v.boundHandle(p, kind, topic, spec), nil
}
