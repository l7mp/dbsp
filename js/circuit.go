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

type circuitTransformOptions struct {
	Pairs [][]string `json:"pairs"`
	Rules string     `json:"rules"`
}

func (h *circuitHandle) register() error {
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

func (h *circuitHandle) doTransform(name string, jsArgs goja.Value) error {
	prevProc := h.proc
	if prevProc != nil {
		h.vm.runtime.Stop(prevProc)
		h.proc = nil
	}

	typ := transform.TransformerType(name)
	var args []any
	if jsArgs != nil && !goja.IsUndefined(jsArgs) && !goja.IsNull(jsArgs) {
		var opts circuitTransformOptions
		if err := decodeOptionValue(jsArgs, &opts); err != nil {
			return fmt.Errorf("transform %s options: %w", typ, err)
		}

		switch typ {
		case transform.Reconciler:
			if len(opts.Pairs) > 0 {
				pairs := make([]transform.ReconcilerPair, 0, len(opts.Pairs))
				for i, p := range opts.Pairs {
					if len(p) != 2 {
						return fmt.Errorf("transform %s: pair %d must have exactly 2 elements", typ, i)
					}
					inputID := strings.TrimSpace(p[0])
					outputID := strings.TrimSpace(p[1])
					if inputID == "" || outputID == "" {
						return fmt.Errorf("transform %s: pair %d must not contain empty values", typ, i)
					}

					// Convenience for JS callers: allow topic names in addition to raw
					// node IDs. Node IDs are generated as input_*/output_*.
					if !strings.HasPrefix(inputID, "input_") {
						inputID = circuit.InputNodeID(inputID)
					}
					if !strings.HasPrefix(outputID, "output_") {
						outputID = circuit.OutputNodeID(outputID)
					}

					pairs = append(pairs, transform.ReconcilerPair{InputID: inputID, OutputID: outputID})
				}
				args = append(args, pairs)
			}
		case transform.Rewriter:
			if opts.Rules != "" {
				args = append(args, opts.Rules)
			}
		}
	}

	t, err := transform.New(typ, args...)
	if err != nil {
		if prevProc != nil {
			h.proc = prevProc
			if regErr := h.register(); regErr != nil {
				return fmt.Errorf("transform: %w (restore failed: %v)", err, regErr)
			}
		}
		return fmt.Errorf("transform: %w", err)
	}

	result, err := t.Transform(h.c)
	if err != nil {
		if prevProc != nil {
			h.proc = prevProc
			if regErr := h.register(); regErr != nil {
				return fmt.Errorf("transform %s: %w (restore failed: %v)", typ, err, regErr)
			}
		}
		return fmt.Errorf("transform %s: %w", typ, err)
	}

	prevCircuit := h.c
	h.c = result
	if err := h.register(); err != nil {
		h.c = prevCircuit
		if prevProc != nil {
			h.proc = prevProc
			if regErr := h.installObserver(); regErr != nil {
				return fmt.Errorf("transform %s: register transformed circuit: %w (restore failed: %v)", typ, err, regErr)
			}
		}
		return fmt.Errorf("transform %s: register transformed circuit: %w", typ, err)
	}

	return nil
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

	if h.proc != nil {
		return nil
	}

	return h.register()
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

	_ = obj.Set("transform", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) < 1 {
			return nil, fmt.Errorf("circuit.transform(name[, opts]) requires transformer name")
		}

		name := call.Argument(0).String()
		var jsArgs goja.Value
		if len(call.Arguments) > 1 {
			jsArgs = call.Argument(1)
		}

		err := h.doTransform(name, jsArgs)
		if err != nil {
			return nil, err
		}
		return obj, nil
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

	_ = obj.Set("toJSON", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		payload := map[string]any{
			"kind":        "circuit",
			"name":        h.c.Name(),
			"validated":   h.proc != nil,
			"observed":    h.obsFn != nil,
			"incremental": true,
		}
		return h.vm.rt.ToValue(payload), nil
	}))

	return obj
}
