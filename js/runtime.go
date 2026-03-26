package main

import (
	"fmt"
	"sync"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/engine/circuit"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

func (v *VM) publish(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("publish(topic, entries) requires topic and entries")
	}

	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("publish: empty topic")
	}

	entries, err := v.fromJSEntries(call.Argument(1))
	if err != nil {
		return nil, fmt.Errorf("publish: %w", err)
	}

	if err := v.runtime.NewPublisher().Publish(dbspruntime.Event{Name: topic, Data: entries}); err != nil {
		return nil, fmt.Errorf("publish: %w", err)
	}

	return goja.Undefined(), nil
}

func (v *VM) subscribe(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("subscribe(topic, fn) requires topic and callback")
	}

	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("subscribe: empty topic")
	}

	jsFn, ok := goja.AssertFunction(call.Argument(1))
	if !ok {
		return nil, fmt.Errorf("subscribe callback must be a function")
	}

	v.registerCallbackConsumer(topic, jsFn)
	return goja.Undefined(), nil
}

func (v *VM) runtimeOnError(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("runtime.onError(fn) requires a callback")
	}

	h, ok := goja.AssertFunction(call.Argument(0))
	if !ok {
		return nil, fmt.Errorf("runtime.onError callback must be a function")
	}

	v.setRuntimeErrorHandler(h)
	return goja.Undefined(), nil
}

func (v *VM) cancel(call goja.FunctionCall) (goja.Value, error) {
	ctx := v.currentCancelContext()
	if err := ctx.Cancel(); err != nil {
		return nil, err
	}

	return goja.Undefined(), nil
}

func (v *VM) runtimeObserve(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("runtime.observe(circuitName, fn) requires circuit name and callback")
	}

	name := call.Argument(0).String()
	if name == "" {
		return nil, fmt.Errorf("runtime.observe: empty circuit name")
	}

	arg := call.Argument(1)
	if goja.IsUndefined(arg) || goja.IsNull(arg) {
		if !v.runtime.SetCircuitObserver(name, nil) {
			return nil, fmt.Errorf("runtime.observe: circuit %q not found", name)
		}
		return goja.Undefined(), nil
	}

	jsFn, ok := goja.AssertFunction(arg)
	if !ok {
		return nil, fmt.Errorf("runtime.observe callback must be a function")
	}

	done := false
	var doneMu sync.RWMutex
	markDone := cancelContextFunc(func() error {
		doneMu.Lock()
		done = true
		doneMu.Unlock()
		if !v.runtime.SetCircuitObserver(name, nil) {
			return fmt.Errorf("runtime.observe: circuit %q not found", name)
		}
		return nil
	})

	obs := func(node *circuit.Node, values map[string]zset.ZSet, schedule []string, position int) {
		doneMu.RLock()
		if done {
			doneMu.RUnlock()
			return
		}
		doneMu.RUnlock()

		payload, err := v.observerPayload(node, values, schedule, position)
		if err != nil {
			v.logger.Error(err, "runtime observer payload conversion failed", "circuit", name)
			return
		}

		v.schedule(func() {
			v.withCancelContext(markDone, func() {
				if _, err := jsFn(goja.Undefined(), v.rt.ToValue(payload)); err != nil {
					v.logger.Error(err, "runtime observer callback failed", "circuit", name)
				}
			})
		})
	}

	if !v.runtime.SetCircuitObserver(name, obs) {
		return nil, fmt.Errorf("runtime.observe: circuit %q not found", name)
	}

	return goja.Undefined(), nil
}
