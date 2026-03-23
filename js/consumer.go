package main

import (
	"fmt"

	"github.com/dop251/goja"
)

func (v *VM) genericConsumer(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("consumer(topic, fn) requires topic and callback")
	}

	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("consumer: empty topic")
	}

	jsFn, ok := goja.AssertFunction(call.Argument(1))
	if !ok {
		return nil, fmt.Errorf("consumer callback must be a function")
	}

	v.registerCallbackConsumer(topic, jsFn)

	return goja.Undefined(), nil
}

func (v *VM) registerCallbackConsumer(topic string, jsFn goja.Callable) {

	sub := v.runtime.NewSubscriber()
	sub.Subscribe(topic)

	go func() {
		for event := range sub.GetChannel() {
			entries, err := v.toJSEntries(event.Data)
			if err != nil {
				v.logger.Error(err, "consumer conversion failed", "topic", topic)
				continue
			}

			v.schedule(func() {
				if _, err := jsFn(goja.Undefined(), v.rt.ToValue(entries)); err != nil {
					v.logger.Error(err, "consumer callback failed", "topic", topic)
				}
			})
		}
	}()
}

func (v *VM) redisConsumer(call goja.FunctionCall) (goja.Value, error) {
	return nil, fmt.Errorf("consumer.redis is not implemented yet")
}
