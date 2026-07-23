package js

import (
	"errors"
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
	c       *circuit.Circuit
	query   *compiler.Query
	vm      *VM
	proc    *dbspruntime.Circuit
	obsFn   goja.Callable
	applied []transform.TransformerType
}

func (h *circuitHandle) hasApplied(typ transform.TransformerType) bool {
	for _, t := range h.applied {
		if t == typ {
			return true
		}
	}
	return false
}

type circuitTransformOptions struct {
	Pairs [][]string `json:"pairs"`
	Rules string     `json:"rules"`
	K     int        `json:"k"`
}

// transformEntry is the .transform() argument: a transformer name with its
// options inline, {name: "...", ...opts}. A bare string is shorthand for an
// entry with no options.
type transformEntry struct {
	Name string `json:"name"`
	circuitTransformOptions
}

func parseTransformEntry(arg goja.Value) (transformEntry, error) {
	var e transformEntry
	if arg == nil || goja.IsUndefined(arg) || goja.IsNull(arg) {
		return e, fmt.Errorf("missing transformer entry")
	}
	if name, ok := arg.Export().(string); ok {
		e.Name = name
	} else if err := decodeOptionValue(arg, &e); err != nil {
		return e, fmt.Errorf("transform entry: %w", err)
	}
	e.Name = strings.TrimSpace(e.Name)
	if e.Name == "" {
		return e, fmt.Errorf("empty transformer name")
	}
	return e, nil
}

// parseTopicPairs converts JS-side [input, output] topic-name pairs into
// ReconcilerPairs with canonical node IDs.
func parseTopicPairs(typ transform.TransformerType, raw [][]string) ([]transform.ReconcilerPair, error) {
	pairs := make([]transform.ReconcilerPair, 0, len(raw))
	for i, p := range raw {
		if len(p) != 2 {
			return nil, fmt.Errorf("transform %s: pair %d must have exactly 2 elements", typ, i)
		}
		inputID := strings.TrimSpace(p[0])
		outputID := strings.TrimSpace(p[1])
		if inputID == "" || outputID == "" {
			return nil, fmt.Errorf("transform %s: pair %d must not contain empty values", typ, i)
		}
		if !strings.HasPrefix(inputID, "input_") {
			inputID = circuit.InputNodeID(inputID)
		}
		if !strings.HasPrefix(outputID, "output_") {
			outputID = circuit.OutputNodeID(outputID)
		}
		pairs = append(pairs, transform.ReconcilerPair{InputID: inputID, OutputID: outputID})
	}
	return pairs, nil
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

// parseTransformArgs converts JS-side transform options into the argument
// list transform.New expects for the given transformer type.
func parseTransformArgs(typ transform.TransformerType, jsOpts circuitTransformOptions) ([]any, error) {
	var args []any

	switch typ {
	case transform.Incrementalizer:
	case transform.Regularizer:
	case transform.Rewriter:
		if strings.TrimSpace(jsOpts.Rules) != "" {
			switch strings.TrimSpace(jsOpts.Rules) {
			case "Pre":
				args = append(args, "Pre")
			case "Post":
				args = append(args, "Post")
			case "Default":
				args = append(args, "Default")
			default:
				return nil, fmt.Errorf("transform %s options: unknown ruleset %q", typ, jsOpts.Rules)
			}
		}
	case transform.Reconciler:
		if len(jsOpts.Pairs) > 0 {
			pairs, err := parseTopicPairs(typ, jsOpts.Pairs)
			if err != nil {
				return nil, err
			}
			args = append(args, pairs)
		}
	case transform.SmithPredictor:
		if len(jsOpts.Pairs) > 0 {
			pairs, err := parseTopicPairs(typ, jsOpts.Pairs)
			if err != nil {
				return nil, err
			}
			args = append(args, pairs)
		}
		args = append(args, jsOpts.K)
	case transform.Optimizer:
		o := transform.OptimizerOptions{}
		if len(jsOpts.Pairs) > 0 {
			pairs, err := parseTopicPairs(typ, jsOpts.Pairs)
			if err != nil {
				return nil, err
			}
			o.ReconcilerPairs = pairs
		}
		if strings.TrimSpace(jsOpts.Rules) != "" {
			switch strings.TrimSpace(jsOpts.Rules) {
			case "Pre":
				o.RewriterRules = transform.PreRules()
			case "Post":
				o.RewriterRules = transform.PostRules()
			case "Default":
				o.RewriterRules = transform.DefaultRules()
			default:
				return nil, fmt.Errorf("transform %s options: unknown ruleset %q", typ, jsOpts.Rules)
			}
		}
		args = append(args, o)
	}

	return args, nil
}

// applyTransformer runs a transformer over the live circuit and installs the
// result atomically: on any error the previous circuit and processor are
// restored.
func (h *circuitHandle) applyTransformer(label string, t transform.Transformer) error {
	prevProc := h.proc
	if prevProc != nil {
		h.vm.runtime.Stop(prevProc)
		h.proc = nil
	}

	result, err := t.Transform(h.c)
	if err != nil {
		if prevProc != nil {
			h.proc = prevProc
			if regErr := h.register(); regErr != nil {
				return errors.Join(fmt.Errorf("transform %s: %w", label, err), fmt.Errorf("restore failed: %w", regErr))
			}
		}
		return fmt.Errorf("transform %s: %w", label, err)
	}

	prevCircuit := h.c
	h.c = result

	if err := h.register(); err != nil {
		h.c = prevCircuit
		if prevProc != nil {
			h.proc = prevProc
			if regErr := h.installObserver(); regErr != nil {
				return errors.Join(
					fmt.Errorf("transform %s: register transformed circuit: %w", label, err),
					fmt.Errorf("restore failed: %w", regErr),
				)
			}
		}
		return fmt.Errorf("transform %s: register transformed circuit: %w", label, err)
	}

	return nil
}

func (h *circuitHandle) doTransform(entry transformEntry) error {
	typ := transform.TransformerType(entry.Name)

	// The Smith compensator is a snapshot-side construction: the distinct
	// it injects must be compiled by the Incrementalizer, so applying it to
	// an already-incremental circuit would run the distinct on delta
	// streams.
	if typ == transform.SmithPredictor &&
		(h.hasApplied(transform.Incrementalizer) || h.hasApplied(transform.Optimizer)) {
		return fmt.Errorf("transform %s: the circuit is already incremental", typ)
	}

	args, err := parseTransformArgs(typ, entry.circuitTransformOptions)
	if err != nil {
		return err
	}

	t, err := transform.New(typ, args...)
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}

	if err := h.applyTransformer(string(typ), t); err != nil {
		return err
	}

	h.applied = append(h.applied, typ)
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

// close unregisters the circuit's runtime processor, clearing any active
// observer. Idempotent: calling close on an already-closed handle is a no-op.
// After close, the handle remains usable: validate() will re-register the
// circuit with the runtime.
func (h *circuitHandle) close() error {
	if h.proc == nil {
		return nil
	}
	if h.obsFn != nil {
		if err := h.clearObserver(); err != nil {
			return err
		}
	}
	h.vm.runtime.Stop(h.proc)
	h.proc = nil
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
		entries, err := v.toJSEntries(value.ShallowCopy())
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
		if len(call.Arguments) != 1 {
			return nil, fmt.Errorf("circuit.transform(entry) takes a single name or {name, ...} entry")
		}

		entry, err := parseTransformEntry(call.Argument(0))
		if err != nil {
			return nil, err
		}

		if err := h.doTransform(entry); err != nil {
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

	_ = obj.Set("close", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if err := h.close(); err != nil {
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
