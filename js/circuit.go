package js

import (
	"encoding/json"
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
	seq     int // auto-id counter for hand-built nodes
}

// validateCircuit is the single well-formedness validator every circuit-
// producing path funnels through: every .transform() validates its result,
// .validate() is the explicit check, and .commit() validates before
// installing. It returns one formatted error, or nil.
func validateCircuit(c *circuit.Circuit) error {
	errs := c.Validate()
	if len(errs) == 0 {
		return nil
	}
	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		messages = append(messages, err.Error())
	}
	return fmt.Errorf("circuit validation failed: %s", strings.Join(messages, "; "))
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
	K     int        `json:"k"`
}

// transformEntry is the .transform() argument: a transformer name with its
// options inline, {name: "...", ...opts}. Every entry is an object, also
// in the list form, so transform lists serialize uniformly (the same shape
// a CRD field carries).
type transformEntry struct {
	Name string `json:"name"`
	circuitTransformOptions
}

func parseTransformEntry(arg goja.Value) (transformEntry, error) {
	var e transformEntry
	if arg == nil || goja.IsUndefined(arg) || goja.IsNull(arg) {
		return e, fmt.Errorf("missing transformer entry")
	}
	if err := decodeOptionValue(arg, &e); err != nil {
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

// commit validates the circuit and installs it into the runtime, replacing
// any previously installed processor. It is the single install point: every
// other verb (compile, .transform(), .validate()) leaves the handle offline,
// so a circuit starts running exactly when the script says so.
func (h *circuitHandle) commit() error {
	if err := validateCircuit(h.c); err != nil {
		return err
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

// parseTransformArgs converts JS-side transform options into the argument
// list transform.New expects for the given transformer type.
func parseTransformArgs(typ transform.TransformerType, jsOpts circuitTransformOptions) ([]any, error) {
	var args []any

	switch typ {
	case transform.Incrementalizer:
	case transform.Distincter:
	case transform.Rewriter:
		return nil, fmt.Errorf("transform %s: not a user-facing transform", typ)
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
	}

	return args, nil
}

// applyTransformer runs a transformer over the circuit and validates the
// result. It does not install: the transformed circuit reaches the runtime on
// the next commit. A failed transform leaves the handle on its previous
// circuit, so the transform chain is all-or-nothing.
func (h *circuitHandle) applyTransformer(label string, t transform.Transformer) error {
	result, err := t.Transform(h.c)
	if err != nil {
		return fmt.Errorf("transform %s: %w", label, err)
	}

	if err := validateCircuit(result); err != nil {
		return fmt.Errorf("transform %s: %w", label, err)
	}

	h.c = result
	return nil
}

func (h *circuitHandle) doTransform(entry transformEntry) error {
	typ := transform.TransformerType(entry.Name)

	// The Smith compensator is a snapshot-side construction: the distinct
	// it injects must be compiled by the Incrementalizer, so applying it to
	// an already-incremental circuit would run the distinct on delta
	// streams.
	if typ == transform.SmithPredictor && h.hasApplied(transform.Incrementalizer) {
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

// doTransformChain applies a set of transform entries as one atomic step,
// in canonical order regardless of the order given (transform.NewChain owns
// the ordering). The circuit swap is atomic: all transforms apply, or none.
func (h *circuitHandle) doTransformChain(raw []any) error {
	if len(raw) == 0 {
		return fmt.Errorf("circuit.transform([...]) requires at least one transform")
	}

	specs := make([]transform.Spec, 0, len(raw))
	for i, el := range raw {
		if _, ok := el.(string); ok {
			return fmt.Errorf("transform list entry %d: expected a {name, ...} object, got a string", i)
		}
		var entry transformEntry
		data, err := json.Marshal(el)
		if err != nil {
			return fmt.Errorf("transform list entry %d: %w", i, err)
		}
		if err := json.Unmarshal(data, &entry); err != nil {
			return fmt.Errorf("transform list entry %d: %w", i, err)
		}
		entry.Name = strings.TrimSpace(entry.Name)
		if entry.Name == "" {
			return fmt.Errorf("transform list entry %d: missing transformer name", i)
		}
		typ := transform.TransformerType(entry.Name)
		args, err := parseTransformArgs(typ, entry.circuitTransformOptions)
		if err != nil {
			return err
		}
		specs = append(specs, transform.Spec{Type: typ, Args: args})
	}

	ch, err := transform.NewChain(specs...)
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}

	for _, s := range ch.Specs() {
		if s.Type == transform.SmithPredictor && h.hasApplied(transform.Incrementalizer) {
			return fmt.Errorf("transform %s: the circuit is already incremental", s.Type)
		}
	}

	if err := h.applyTransformer("Chain", ch); err != nil {
		return err
	}

	for _, s := range ch.Specs() {
		h.applied = append(h.applied, s.Type)
	}
	return nil
}

// close unregisters the circuit's runtime processor, clearing any active
// observer. Idempotent: calling close on an already-closed handle is a no-op.
// After close, the handle remains usable: commit() re-installs the circuit.
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
			return nil, fmt.Errorf("circuit.transform(entry) takes a single {name, ...} entry or a list of entries")
		}

		arg := call.Argument(0)
		if list, ok := arg.Export().([]any); ok {
			if err := h.doTransformChain(list); err != nil {
				return nil, err
			}
			return obj, nil
		}

		entry, err := parseTransformEntry(arg)
		if err != nil {
			return nil, err
		}

		if err := h.doTransform(entry); err != nil {
			return nil, err
		}
		return obj, nil
	}))

	_ = obj.Set("node", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) < 1 || goja.IsUndefined(call.Argument(0)) || goja.IsNull(call.Argument(0)) {
			return nil, fmt.Errorf("circuit.node(spec[, id]) requires an operator spec")
		}
		id := ""
		if len(call.Arguments) > 1 && !goja.IsUndefined(call.Argument(1)) && !goja.IsNull(call.Argument(1)) {
			id = strings.TrimSpace(call.Argument(1).String())
		}
		nid, err := h.addNode(call.Argument(0).Export(), id)
		if err != nil {
			return nil, err
		}
		return h.vm.rt.ToValue(nid), nil
	}))

	_ = obj.Set("input", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) < 1 {
			return nil, fmt.Errorf("circuit.input(topic) requires a topic name")
		}
		id, err := h.addBoundary(call.Argument(0).String(), true)
		if err != nil {
			return nil, err
		}
		return h.vm.rt.ToValue(id), nil
	}))

	_ = obj.Set("output", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) < 1 {
			return nil, fmt.Errorf("circuit.output(topic) requires a topic name")
		}
		id, err := h.addBoundary(call.Argument(0).String(), false)
		if err != nil {
			return nil, err
		}
		return h.vm.rt.ToValue(id), nil
	}))

	_ = obj.Set("edge", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) < 2 {
			return nil, fmt.Errorf("circuit.edge(from, to[, port]) requires two node IDs")
		}
		port := 0
		if len(call.Arguments) > 2 && !goja.IsUndefined(call.Argument(2)) && !goja.IsNull(call.Argument(2)) {
			port = int(call.Argument(2).ToInteger())
		}
		if err := h.addEdge(call.Argument(0).String(), call.Argument(1).String(), port); err != nil {
			return nil, err
		}
		return obj, nil
	}))

	_ = obj.Set("commit", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if err := h.commit(); err != nil {
			return nil, err
		}
		return obj, nil
	}))

	_ = obj.Set("validate", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if err := validateCircuit(h.c); err != nil {
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
			"committed":   h.proc != nil,
			"observed":    h.obsFn != nil,
			"incremental": h.hasApplied(transform.Incrementalizer),
		}
		return h.vm.rt.ToValue(payload), nil
	}))

	return obj
}
