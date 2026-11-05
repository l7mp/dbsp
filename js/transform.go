package js

import (
	"encoding/json"
	"fmt"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/engine/transform"
)

// transformHandle is the JavaScript leg of one transform entry: the entry
// itself, built by transform.create({name, ...}) and handed to
// circuit.transform(), or looked up on a loaded operator with
// runtime.transform(circuit, name). A closed-loop transform's handle owns
// the loop's switch: enable(on) sets the initial position while the entry
// is not committed yet and flips the running circuit's switch once it is.
type transformHandle struct {
	vm   *VM
	spec transform.TransformSpec

	// circuit and index bind a created entry to the circuit handle that
	// recorded it (circuit.transform), at that position of its chain.
	circuit *circuitHandle
	index   int
	// inst and circuitName bind a looked-up entry to a circuit of a loaded
	// operator (runtime.transform).
	inst        *runtimeInstance
	circuitName string
}

// enable sets the position of the transform's loop switch and returns
// the number of switches set.
func (t *transformHandle) enable(on bool) (int, error) {
	typ := transform.TransformerType(t.spec.Name)
	if _, ok := transform.LoopPrefix(typ); !ok {
		return 0, fmt.Errorf("transform %s has no switch", t.spec.Name)
	}
	if t.inst != nil {
		return t.inst.rt.SetCircuitEnabled(t.circuitName, typ, on)
	}
	if t.circuit != nil && t.circuit.proc != nil {
		return t.circuit.proc.SetEnabled(typ, on)
	}
	v := on
	t.spec.Enabled = &v
	if t.circuit != nil {
		t.circuit.transformSpecs[t.index].Enabled = &v
	}
	return 1, nil
}

func (t *transformHandle) jsObject() *goja.Object {
	v := t.vm
	obj := v.rt.NewObject()
	_ = obj.Set("__handle", v.rt.ToValue(t))
	_ = obj.Set("name", v.rt.ToValue(t.spec.Name))
	_ = obj.Set("enable", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		on := true
		if len(call.Arguments) > 0 && !goja.IsUndefined(call.Argument(0)) && !goja.IsNull(call.Argument(0)) {
			on = call.Argument(0).ToBoolean()
		}
		n, err := t.enable(on)
		if err != nil {
			return nil, err
		}
		return v.rt.ToValue(n), nil
	}))
	_ = obj.Set("toJSON", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		data, err := json.Marshal(t.spec)
		if err != nil {
			return nil, err
		}
		var entry map[string]any
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, err
		}
		return v.rt.ToValue(entry), nil
	}))
	return obj
}

// transformHandleOf returns the transform handle a JavaScript value
// carries, if it is one.
func transformHandleOf(val any) (*transformHandle, bool) {
	switch x := val.(type) {
	case *transformHandle:
		return x, true
	case map[string]any:
		t, ok := x["__handle"].(*transformHandle)
		return t, ok
	case goja.Value:
		if x == nil || goja.IsUndefined(x) || goja.IsNull(x) {
			return nil, false
		}
		obj, ok := x.(*goja.Object)
		if !ok {
			return nil, false
		}
		h := obj.Get("__handle")
		if h == nil {
			return nil, false
		}
		t, ok := h.Export().(*transformHandle)
		return t, ok
	}
	return nil, false
}

// makeTransformObject builds the transform global: create(entry) returns
// the handle of one transform entry.
func (v *VM) makeTransformObject() *goja.Object {
	obj := v.rt.NewObject()
	_ = obj.Set("create", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) != 1 {
			return nil, fmt.Errorf("transform.create(entry) takes a single {name, ...} entry")
		}
		entry, err := parseTransformEntry(call.Argument(0))
		if err != nil {
			return nil, err
		}
		if _, err := entry.Spec(); err != nil {
			return nil, fmt.Errorf("transform: %w", err)
		}
		return (&transformHandle{vm: v, spec: entry}).jsObject(), nil
	}))
	return obj
}
