package main

import (
	"context"
	"fmt"

	"github.com/dop251/goja"
)

// subscribe(topic, fn) — registers fn as a sink callback for topic.
// fn receives [[doc, weight], ...] entries; its return value is ignored.
func (v *VM) subscribeDispatch(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("subscribe(topic, fn) requires topic and callback")
	}

	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("subscribe: empty topic")
	}

	jsFn, ok := goja.AssertFunction(call.Argument(1))
	if !ok {
		return nil, fmt.Errorf("subscribe: callback must be a function")
	}

	v.registerCallbackConsumer(topic, jsFn)
	return goja.Undefined(), nil
}

// subscribe.once(topic) — returns a Promise that resolves to the first batch
// of entries published to topic.
func (v *VM) subscribeOnce(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("subscribe.once(topic) requires topic")
	}

	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("subscribe.once: empty topic")
	}

	promise, resolve, reject := v.rt.NewPromise()
	sub := v.runtime.NewSubscriber()
	sub.Subscribe(topic)

	go func() {
		stop := context.AfterFunc(v.ctx, sub.UnsubscribeAll)
		defer stop()
		event, ok := sub.Next()
		if !ok {
			ctxErr := v.ctx.Err()
			v.schedule(func() {
				if ctxErr != nil {
					if err := reject(v.rt.NewGoError(ctxErr)); err != nil {
						v.logger.Error(err, "subscribe.once reject failed", "topic", topic)
					}
				} else {
					if err := reject(v.rt.NewGoError(fmt.Errorf("channel closed"))); err != nil {
						v.logger.Error(err, "subscribe.once reject failed", "topic", topic)
					}
				}
			})
			return
		}

		v.schedule(func() {
			entries, err := v.toJSEntries(event.Data)
			if err != nil {
				if rejectErr := reject(v.rt.NewGoError(err)); rejectErr != nil {
					v.logger.Error(rejectErr, "subscribe.once reject failed", "topic", topic)
				}
				return
			}
			if err := resolve(v.rt.ToValue(entries)); err != nil {
				v.logger.Error(err, "subscribe.once resolve failed", "topic", topic)
			}
		})
	}()

	return v.rt.ToValue(promise), nil
}
