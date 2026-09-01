package js

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	aggcompiler "github.com/l7mp/dbsp/engine/compiler/aggregation"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
)

type circuitHandle struct {
	c       *circuit.Circuit
	query   *compiler.Query
	vm      *VM
	rt      *dbspruntime.Runtime
	proc    *dbspruntime.Circuit
	obsFn   goja.Callable
	applied []transform.TransformerType
	seq     int // auto-id counter for hand-built nodes

	// The handle's serialized form, printed by spec(): the program source
	// (pipeline or sql), the topic bindings, and the transforms applied.
	srcKind        string
	src            json.RawMessage
	bindIn         []aggcompiler.Binding
	bindOut        []aggcompiler.Binding
	specName       string
	transformSpecs []transform.TransformSpec
}

// specValue returns the handle's serialized form: the source program, the
// bindings, and the transform entries applied so far.
func (h *circuitHandle) specValue() map[string]any {
	doc := map[string]any{}
	if h.srcKind != "" && len(h.src) > 0 {
		var parsed any
		if err := json.Unmarshal(h.src, &parsed); err == nil {
			doc[h.srcKind] = parsed
		}
	}
	bindings := func(bs []aggcompiler.Binding) []map[string]any {
		out := make([]map[string]any, 0, len(bs))
		for _, b := range bs {
			out = append(out, map[string]any{"name": b.Name, "logical": b.Logical})
		}
		return out
	}
	if len(h.bindIn) > 0 {
		doc["inputs"] = bindings(h.bindIn)
	}
	if len(h.bindOut) > 0 {
		doc["outputs"] = bindings(h.bindOut)
	}
	if h.specName != "" {
		doc["name"] = h.specName
	}
	if len(h.transformSpecs) > 0 {
		doc["transforms"] = specDocument(struct {
			T []transform.TransformSpec `json:"t"`
		}{h.transformSpecs})["t"]
	}
	return doc
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

// parseTransformEntry decodes a .transform() argument, {name: "...",
// ...opts}: the transform.TransformSpec wire shape, the same one a
// serialized controller (engine/spec) carries. Every entry is an object,
// also in the list form, so transform lists serialize uniformly.
func parseTransformEntry(arg goja.Value) (transform.TransformSpec, error) {
	var e transform.TransformSpec
	if arg == nil || goja.IsUndefined(arg) || goja.IsNull(arg) {
		return e, fmt.Errorf("missing transformer entry")
	}
	if err := decodeOptionValue(arg, &e); err != nil {
		return e, fmt.Errorf("transform entry: %w", err)
	}
	if strings.TrimSpace(e.Name) == "" {
		return e, fmt.Errorf("empty transformer name")
	}
	return e, nil
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

	proc, err := dbspruntime.NewCircuit(h.c.Name(), h.rt, &query, h.vm.logger)
	if err != nil {
		return fmt.Errorf("runtime circuit: %w", err)
	}

	if h.proc != nil {
		h.rt.Stop(h.proc)
	}

	if err := h.rt.Add(proc); err != nil {
		return fmt.Errorf("runtime add circuit: %w", err)
	}

	h.proc = proc
	if err := h.installObserver(); err != nil {
		return err
	}

	return nil
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

func (h *circuitHandle) doTransform(entry transform.TransformSpec) error {
	spec, err := entry.Spec()
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}

	// The Smith compensator is a snapshot-side construction: the distinct
	// it injects must be compiled by the Incrementalizer, so applying it to
	// an already-incremental circuit would run the distinct on delta
	// streams.
	if spec.Type == transform.SmithPredictor && h.hasApplied(transform.Incrementalizer) {
		return fmt.Errorf("transform %s: the circuit is already incremental", spec.Type)
	}

	t, err := transform.New(spec.Type, spec.Args...)
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}

	if err := h.applyTransformer(string(spec.Type), t); err != nil {
		return err
	}

	h.applied = append(h.applied, spec.Type)
	h.transformSpecs = append(h.transformSpecs, entry)
	return nil
}

// doTransformChain applies a set of transform entries as one atomic step,
// in canonical order regardless of the order given (transform.NewChain owns
// the ordering). The circuit swap is atomic: all transforms apply, or none.
func (h *circuitHandle) doTransformChain(raw []any) error {
	if len(raw) == 0 {
		return fmt.Errorf("circuit.transform([...]) requires at least one transform")
	}

	entries := make([]transform.TransformSpec, 0, len(raw))
	for i, el := range raw {
		if _, ok := el.(string); ok {
			return fmt.Errorf("transform list entry %d: expected a {name, ...} object, got a string", i)
		}
		var entry transform.TransformSpec
		data, err := json.Marshal(el)
		if err != nil {
			return fmt.Errorf("transform list entry %d: %w", i, err)
		}
		if err := json.Unmarshal(data, &entry); err != nil {
			return fmt.Errorf("transform list entry %d: %w", i, err)
		}
		entries = append(entries, entry)
	}

	ch, err := transform.NewChainFromSpecs(entries)
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
	h.transformSpecs = append(h.transformSpecs, entries...)
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
	h.rt.Stop(h.proc)
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
		if !h.rt.SetCircuitObserver(h.proc.Name(), nil) {
			return fmt.Errorf("circuit.observe: runtime circuit %q not found", h.proc.Name())
		}
		return nil
	}

	cb := h.obsFn
	done := false
	markDone := cancelContextFunc(func() error {
		done = true
		if !h.rt.SetCircuitObserver(h.proc.Name(), nil) {
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

	if !h.rt.SetCircuitObserver(h.proc.Name(), obs) {
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

	_ = obj.Set("spec", h.vm.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return h.vm.rt.ToValue(h.specValue()), nil
	}))

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
