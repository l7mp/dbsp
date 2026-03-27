package main

import (
	"fmt"
	"sync"

	"github.com/dop251/goja"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
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
	done := make(chan struct{})
	var stopOnce sync.Once
	stop := cancelContextFunc(func() error {
		stopOnce.Do(func() {
			close(done)
			sub.Unsubscribe(topic)
		})
		return nil
	})

	go func() {
		defer stop()
		ch := sub.GetChannel()
		for {
			select {
			case <-done:
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				select {
				case <-done:
					return
				default:
				}

				entries, err := v.toJSEntries(event.Data)
				if err != nil {
					v.logger.Error(err, "consumer conversion failed", "topic", topic)
					continue
				}

				v.schedule(func() {
					select {
					case <-done:
						return
					default:
					}
					v.withCancelContext(stop, func() {
						if _, err := jsFn(goja.Undefined(), v.rt.ToValue(entries)); err != nil {
							v.logger.Error(err, "consumer callback failed", "topic", topic)
						}
					})
				})
			}
		}
	}()
}

func (v *VM) registerTransformCallback(sourceTopic, targetTopic, errorOrigin string, jsFn goja.Callable) {
	sub := v.runtime.NewSubscriber()
	sub.Subscribe(sourceTopic)
	pub := v.runtime.NewPublisher()

	done := make(chan struct{})
	var stopOnce sync.Once
	stop := cancelContextFunc(func() error {
		stopOnce.Do(func() {
			close(done)
			sub.Unsubscribe(sourceTopic)
		})
		return nil
	})

	go func() {
		defer stop()
		ch := sub.GetChannel()
		for {
			select {
			case <-done:
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				select {
				case <-done:
					return
				default:
				}

				input := event.Data.Clone()
				entries, err := v.toJSEntries(input)
				if err != nil {
					v.logger.Error(err, "consumer transform conversion failed", "source_topic", sourceTopic, "target_topic", targetTopic)
					continue
				}

				v.schedule(func() {
					select {
					case <-done:
						return
					default:
					}

					v.withCancelContext(stop, func() {
						result, err := jsFn(goja.Undefined(), v.rt.ToValue(entries))
						if err != nil {
							v.logger.Error(err, "consumer transform callback failed", "source_topic", sourceTopic, "target_topic", targetTopic)
							return
						}

						var out dbspruntime.Event
						switch {
						case goja.IsNull(result):
							return
						case goja.IsUndefined(result):
							out = dbspruntime.Event{Name: targetTopic, Data: input}
						default:
							converted, convErr := v.fromJSEntries(result)
							if convErr != nil {
								v.logger.Error(convErr, "consumer transform callback result decode failed", "source_topic", sourceTopic, "target_topic", targetTopic)
								return
							}
							out = dbspruntime.Event{Name: targetTopic, Data: converted}
						}

						if out.Data.IsZero() {
							return
						}

						if err := pub.Publish(out); err != nil {
							v.runtime.ReportError(errorOrigin, fmt.Errorf("publish transformed event: %w", err))
						}
					})
				})
			}
		}
	}()
}

func (v *VM) redisConsumer(call goja.FunctionCall) (goja.Value, error) {
	return nil, fmt.Errorf("consumer.redis is not implemented yet")
}
