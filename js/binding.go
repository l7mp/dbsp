package js

import (
	"encoding/json"

	"github.com/dop251/goja"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// specDocument serializes a wire spec into a plain JS-facing document via
// its JSON form, the same bytes the spec deserialized from.
func specDocument(spec any) map[string]any {
	b, err := json.Marshal(spec)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

// boundHandle wraps a runnable handle with its serialized binding: spec()
// (and toJSON) return {verb, topic, spec}, the exact wire form the binding
// was constructed from, so any installed binding prints the serialized
// configuration that reproduces it.
func (v *VM) boundHandle(r dbspruntime.Runnable, verb, topic string, spec any, extras ...func()) *goja.Object {
	obj := v.runnableHandle(r, extras...)
	doc := map[string]any{"verb": verb, "topic": topic, "spec": specDocument(spec)}
	printer := v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(doc), nil
	})
	_ = obj.Set("spec", printer)
	_ = obj.Set("toJSON", printer)
	return obj
}
