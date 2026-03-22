package main

import (
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/transform"
)

type circuitHandle struct {
	c     *circuit.Circuit
	query *compiler.Query
	vm    *VM
	proc  *dbspruntime.Circuit
}

func (h *circuitHandle) incrementalize() (*circuitHandle, error) {
	inc, err := transform.Incrementalize(h.c)
	if err != nil {
		return nil, fmt.Errorf("incrementalize: %w", err)
	}
	return &circuitHandle{c: inc, query: h.query, vm: h.vm}, nil
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

	h.vm.runtime.Add(proc)
	h.proc = proc

	return nil
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

	return obj
}
