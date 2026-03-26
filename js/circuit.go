package main

import (
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
)

type circuitHandle struct {
	c     *circuit.Circuit
	query *compiler.Query
	vm    *VM
	proc  *dbspruntime.Circuit
	obsFn goja.Callable
}

func (h *circuitHandle) incrementalize() (*circuitHandle, error) {
	inc, err := transform.Incrementalize(h.c)
	if err != nil {
		return nil, fmt.Errorf("incrementalize: %w", err)
	}
	return &circuitHandle{c: inc, query: h.query, vm: h.vm, obsFn: h.obsFn}, nil
}

func (h *circuitHandle) validate() error {
	errs := h.c.Validate()
	if len(errs) > 0 {
		messages := make([]string, 0, len(errs))
		for _, err := range errs {
			messages = append(messages, err.Error())
		}
		return fmt.Errorf("circuit validation failed: %s", strings.Join(messages, "; "))
	}

	query := *h.query
	query.Circuit = h.c

	proc, err := dbspruntime.NewCircuit(h.c.Name(), h.vm.runtime, &query, h.vm.logger)
	if err != nil {
		return fmt.Errorf("runtime circuit: %w", err)
	}

	if h.proc != nil {
		h.vm.runtime.Stop(h.proc)
	}

	if err := h.vm.runtime.Add(proc); err != nil {
		return fmt.Errorf("runtime add circuit: %w", err)
	}
	h.proc = proc
	if err := h.installObserver(); err != nil {
		return err
	}

	return nil
}

func (h *circuitHandle) observe(jsFn goja.Callable) error {
	h.obsFn = jsFn
	return h.installObserver()
}

func (h *circuitHandle) clearObserver() error {
	h.obsFn = nil
	return h.installObserver()
}

func (h *circuitHandle) installObserver() error {
	if h.proc == nil {
		return nil
	}

	if h.obsFn == nil {
		if !h.vm.runtime.SetCircuitObserver(h.proc.Name(), nil) {
			return fmt.Errorf("circuit.observe: runtime circuit %q not found", h.proc.Name())
		}
		return nil
	}

	cb := h.obsFn
	done := false
	markDone := cancelContextFunc(func() error {
		done = true
		if !h.vm.runtime.SetCircuitObserver(h.proc.Name(), nil) {
			return fmt.Errorf("circuit.observe: runtime circuit %q not found", h.proc.Name())
		}
		return nil
	})
	obs := func(node *circuit.Node, values map[string]zset.ZSet, schedule []string, position int) {
		if done {
			return
		}

		payload, err := h.vm.observerPayload(node, values, schedule, position)
		if err != nil {
			h.vm.logger.Error(err, "circuit observer payload conversion failed", "circuit", h.proc.Name())
			return
		}

		h.vm.schedule(func() {
			h.vm.withCancelContext(markDone, func() {
				if _, err := cb(goja.Undefined(), h.vm.rt.ToValue(payload)); err != nil {
					h.vm.logger.Error(err, "circuit observer callback failed", "circuit", h.proc.Name())
				}
			})
		})
	}

	if !h.vm.runtime.SetCircuitObserver(h.proc.Name(), obs) {
		return fmt.Errorf("circuit.observe: runtime circuit %q not found", h.proc.Name())
	}

	return nil
}

func (v *VM) observerPayload(node *circuit.Node, values map[string]zset.ZSet, schedule []string, position int) (map[string]any, error) {
	serialized := make(map[string]any, len(values))
	for id, value := range values {
		entries, err := v.toJSEntries(value.Clone())
		if err != nil {
			return nil, fmt.Errorf("node %q values: %w", id, err)
		}
		serialized[id] = entries
	}

	scheduleCopy := append([]string(nil), schedule...)

	return map[string]any{
		"node": map[string]any{
			"id":       node.ID,
			"kind":     node.Kind().String(),
			"operator": node.Operator.String(),
		},
		"position": position,
		"schedule": scheduleCopy,
		"values":   serialized,
	}, nil
}

func (h *circuitHandle) jsObject() *goja.Object {
	obj := h.vm.rt.NewObject()

	_ = obj.Set("incrementalize", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		next, err := h.incrementalize()
		if err != nil {
			return nil, err
		}
		return next.jsObject(), nil
	}))

	_ = obj.Set("validate", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if err := h.validate(); err != nil {
			return nil, err
		}
		return obj, nil
	}))

	_ = obj.Set("observe", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) < 1 {
			return nil, fmt.Errorf("circuit.observe(fn) requires a callback")
		}

		arg := call.Argument(0)
		if goja.IsUndefined(arg) || goja.IsNull(arg) {
			if err := h.clearObserver(); err != nil {
				return nil, err
			}
			return obj, nil
		}

		fn, ok := goja.AssertFunction(arg)
		if !ok {
			return nil, fmt.Errorf("circuit.observe callback must be a function")
		}

		if err := h.observe(fn); err != nil {
			return nil, err
		}

		return obj, nil
	}))

	return obj
}
