package js

import (
	"fmt"
	"time"

	"github.com/dop251/goja"

	misc "github.com/l7mp/dbsp/connectors/misc"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// The misc namespace binds the misc connector's trigger producers:
// `misc.tick(topic, {kind, name, namespace, period})` emits a trigger
// document every period, `misc.init(topic, {kind, name, namespace})` emits
// one at startup. Every emission retracts the previous trigger document,
// so the topic is a well-formed state stream holding exactly the current
// trigger; timers sharing a topic are distinguished by name. The kind
// names one of the connector's resource kinds (Timer today) and is
// carried by the trigger documents.

type miscTriggerOptions struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Period    string `json:"period"`
}

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

func (v *VM) miscTriggerArgs(kind string, call goja.FunctionCall) (string, miscTriggerOptions, error) {
	topic, ok := call.Argument(0).Export().(string)
	if !ok || topic == "" {
		return "", miscTriggerOptions{}, fmt.Errorf("%s(topic, opts): expected non-empty string topic, got %s", kind, describeCall(call))
	}
	var opts miscTriggerOptions
	if len(call.Arguments) > 1 {
		if err := decodeOptionValue(call.Argument(1), &opts); err != nil {
			return "", miscTriggerOptions{}, fmt.Errorf("%s options: %w", kind, err)
		}
	}
	if opts.Kind == "" {
		return "", miscTriggerOptions{}, fmt.Errorf("%s: kind is required", kind)
	}
	if opts.Kind != "Timer" {
		return "", miscTriggerOptions{}, fmt.Errorf("%s: unknown kind %q", kind, opts.Kind)
	}
	return topic, opts, nil
}

func (v *VM) miscTick(call goja.FunctionCall) (goja.Value, error) {
	topic, opts, err := v.miscTriggerArgs("misc.tick", call)
	if err != nil {
		return nil, err
	}
	if opts.Period == "" {
		return nil, fmt.Errorf("misc.tick: period is required")
	}
	period, err := time.ParseDuration(opts.Period)
	if err != nil {
		return nil, fmt.Errorf("misc.tick: period: %w", err)
	}
	p, err := misc.NewPeriodicProducer(misc.PeriodicConfig{
		Name:        fmt.Sprintf("misc/tick/%s", topic),
		InputName:   topic,
		TriggerKind: opts.Kind,
		Namespace:   opts.Namespace,
		TriggerName: opts.Name,
		Period:      period,
		Runtime:     v.runtime,
		Logger:      v.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("misc.tick: %w", err)
	}
	if err := v.runtime.Add(p); err != nil {
		return nil, fmt.Errorf("misc.tick: register producer: %w", err)
	}
	var _ dbspruntime.Runnable = p
	return v.runnableHandle(p), nil
}

func (v *VM) miscInit(call goja.FunctionCall) (goja.Value, error) {
	topic, opts, err := v.miscTriggerArgs("misc.init", call)
	if err != nil {
		return nil, err
	}
	p, err := misc.NewOneShotProducer(misc.OneShotConfig{
		Name:        fmt.Sprintf("misc/init/%s", topic),
		InputName:   topic,
		TriggerKind: opts.Kind,
		Namespace:   opts.Namespace,
		TriggerName: opts.Name,
		Runtime:     v.runtime,
		Logger:      v.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("misc.init: %w", err)
	}
	if err := v.runtime.Add(p); err != nil {
		return nil, fmt.Errorf("misc.init: register producer: %w", err)
	}
	return v.runnableHandle(p), nil
}
