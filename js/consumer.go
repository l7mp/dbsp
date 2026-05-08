package js

import (
	"context"
	"fmt"
	"sync"

	"github.com/dop251/goja"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// registerCallbackConsumer subscribes to topic and calls jsFn with each batch.
// jsFn is a sink: its return value is discarded.
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
		stopSub := context.AfterFunc(v.ctx, sub.UnsubscribeAll)
		defer stopSub()
		for {
			event, ok := sub.Next()
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
				v.logger.Error(err, "subscribe callback: entries conversion failed", "topic", topic)
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
						v.logger.Error(err, "subscribe callback failed", "topic", topic)
					}
				})
			})
		}
	}()
}

// registerProducerCallback subscribes to sourceTopic, calls jsFn with each
// batch, and publishes the return value to targetTopic. A JS return of
// undefined or null publishes an empty Z-set (producer semantics: the callback
// transforms raw data before it hits the bus). The returned stop function
// cancels the subscription and is safe to call multiple times.
func (v *VM) registerProducerCallback(sourceTopic, targetTopic, errorOrigin string, jsFn goja.Callable) func() {
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
		stopSub := context.AfterFunc(v.ctx, sub.UnsubscribeAll)
		defer stopSub()
		for {
			event, ok := sub.Next()
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
				v.logger.Error(err, "producer callback: entries conversion failed",
					"source_topic", sourceTopic, "target_topic", targetTopic)
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
						v.logger.Error(err, "producer callback failed",
							"source_topic", sourceTopic, "target_topic", targetTopic)
						return
					}

					var outData zset.ZSet
					if goja.IsNull(result) || goja.IsUndefined(result) {
						outData = zset.New()
					} else {
						converted, convErr := v.fromJSEntries(result)
						if convErr != nil {
							v.logger.Error(convErr, "producer callback result decode failed",
								"source_topic", sourceTopic, "target_topic", targetTopic)
							return
						}
						outData = converted
					}

					out := dbspruntime.Event{Name: targetTopic, Data: outData}
					if err := pub.Publish(out); err != nil {
						v.runtime.ReportError(errorOrigin, fmt.Errorf("publish producer callback event: %w", err))
					}
				})
			})
		}
	}()

	return func() { _ = stop.Cancel() }
}
