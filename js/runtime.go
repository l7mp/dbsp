package js

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/engine/circuit"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// stats implements the stats verb: the instance runtime's snapshot
// (its counters, the process counters and the Go runtime figures). The
// JSON round trip honors the snapshot's json tags, so JS sees the
// documented camelCase field names.
func (inst *runtimeInstance) stats(call goja.FunctionCall) (goja.Value, error) {
	b, err := json.Marshal(inst.rt.Stats())
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return inst.vm.rt.ToValue(out), nil
}

// publish implements the publish verb: bound naked for the default
// runtime, a handle method for created ones.
func (inst *runtimeInstance) publish(call goja.FunctionCall) (goja.Value, error) {
	v := inst.vm
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

	if err := inst.rt.Publish(dbspruntime.Event{Name: topic, Data: entries}); err != nil {
		return nil, fmt.Errorf("publish: %w", err)
	}

	return goja.Undefined(), nil
}

// onError installs the runtime's error handler.
func (inst *runtimeInstance) onError(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("onError(fn) requires a callback")
	}

	h, ok := goja.AssertFunction(call.Argument(0))
	if !ok {
		return nil, fmt.Errorf("onError callback must be a function")
	}

	inst.mu.Lock()
	inst.errHandler = h
	inst.mu.Unlock()
	return goja.Undefined(), nil
}

func (v *VM) cancel(call goja.FunctionCall) (goja.Value, error) {
	ctx := v.currentCancelContext()
	if err := ctx.Cancel(); err != nil {
		return nil, err
	}

	return goja.Undefined(), nil
}

// observe attaches a circuit observer on this runtime.
func (inst *runtimeInstance) observe(call goja.FunctionCall) (goja.Value, error) {
	v := inst.vm
	rt := inst.rt
	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("runtime.observe(circuitName, fn) requires circuit name and callback")
	}

	name := call.Argument(0).String()
	if name == "" {
		return nil, fmt.Errorf("runtime.observe: empty circuit name")
	}

	arg := call.Argument(1)
	if goja.IsUndefined(arg) || goja.IsNull(arg) {
		if !rt.SetCircuitObserver(name, nil) {
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
		if !rt.SetCircuitObserver(name, nil) {
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

	if !rt.SetCircuitObserver(name, obs) {
		return nil, fmt.Errorf("runtime.observe: circuit %q not found", name)
	}

	return goja.Undefined(), nil
}
