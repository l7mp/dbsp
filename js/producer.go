package main

import (
	"fmt"

	"github.com/dop251/goja"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

type producerHandle struct {
	pub   dbspruntime.Publisher
	topic string
	vm    *VM
}

func (h *producerHandle) publish(arg goja.Value) error {
	entries, err := h.vm.fromJSEntries(arg)
	if err != nil {
		return err
	}
	return h.pub.Publish(dbspruntime.Event{Name: h.topic, Data: entries})
}

func (h *producerHandle) jsObject() *goja.Object {
	obj := h.vm.rt.NewObject()
	_ = obj.Set("publish", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) < 1 {
			return nil, fmt.Errorf("producer.publish(entries) requires entries")
		}
		if err := h.publish(call.Argument(0)); err != nil {
			return nil, err
		}
		return goja.Undefined(), nil
	}))
	return obj
}

func (v *VM) genericProducer(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("producer(topic[, entries]) requires topic")
	}

	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("producer: empty topic")
	}

	h := &producerHandle{pub: v.runtime.NewPublisher(), topic: topic, vm: v}
	if len(call.Arguments) > 1 {
		if err := h.publish(call.Argument(1)); err != nil {
			return nil, err
		}
	}

	return h.jsObject(), nil
}

func (v *VM) jsonlProducer(call goja.FunctionCall) (goja.Value, error) {
	return nil, fmt.Errorf("producer.jsonl is not implemented yet")
}
