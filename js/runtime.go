package main

import (
	"fmt"

	"github.com/dop251/goja"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
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
