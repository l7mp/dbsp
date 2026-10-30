package js

import (
	"fmt"

	"github.com/dop251/goja"

	misccodec "github.com/l7mp/dbsp/connectors/misc/codec"
)

// formatApply is the shared implementation for all format.* JS methods: it
// decodes the argument as JS entries, applies the codec function newFn
// builds, and returns the result. name is the JS method name for errors.
func (v *VM) formatApply(name string, newFn func(errFn func(error)) misccodec.ZSetFunc, call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("format.%s(entries) requires entries", name)
	}
	z, err := v.fromJSEntries(call.Argument(0))
	if err != nil {
		return nil, fmt.Errorf("format.%s: %w", name, err)
	}
	errFn := func(err error) {
		v.logger.Error(err, "format error", "format", name)
	}
	out, err := newFn(errFn)(z)
	if err != nil {
		return nil, fmt.Errorf("format.%s: %w", name, err)
	}
	entries, err := v.toJSEntries(out)
	if err != nil {
		return nil, fmt.Errorf("format.%s: encode result: %w", name, err)
	}
	return v.rt.ToValue(entries), nil
}

// formatParse parses the "line" field of every entry with the codec of format.
func (v *VM) formatParse(format misccodec.Format, call goja.FunctionCall) (goja.Value, error) {
	return v.formatApply(string(format), func(errFn func(error)) misccodec.ZSetFunc {
		return misccodec.ParseFunc(format, errFn)
	}, call)
}

// formatToYAML serializes every entry's document as YAML into its "line" field.
func (v *VM) formatToYAML(call goja.FunctionCall) (goja.Value, error) {
	return v.formatApply("toYAML", func(errFn func(error)) misccodec.ZSetFunc {
		return misccodec.FormatFunc(misccodec.FormatYAML, errFn)
	}, call)
}

func (v *VM) formatJSONL(call goja.FunctionCall) (goja.Value, error) {
	return v.formatParse(misccodec.FormatJSONL, call)
}

func (v *VM) formatJSON(call goja.FunctionCall) (goja.Value, error) {
	return v.formatParse(misccodec.FormatJSON, call)
}

func (v *VM) formatYAML(call goja.FunctionCall) (goja.Value, error) {
	return v.formatParse(misccodec.FormatYAML, call)
}

func (v *VM) formatCSV(call goja.FunctionCall) (goja.Value, error) {
	return v.formatParse(misccodec.FormatCSV, call)
}

func (v *VM) formatAuto(call goja.FunctionCall) (goja.Value, error) {
	return v.formatParse(misccodec.FormatAuto, call)
}
