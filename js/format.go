package main

import (
	"fmt"

	"github.com/dop251/goja"

	misccodec "github.com/l7mp/dbsp/connectors/misc/codec"
)

// formatParse is the shared implementation for all format.* JS methods.
// It decodes the argument as JS entries, applies fn, and returns the result.
func (v *VM) formatParse(format misccodec.Format, call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("format.%s(entries) requires entries", format)
	}
	z, err := v.fromJSEntries(call.Argument(0))
	if err != nil {
		return nil, fmt.Errorf("format.%s: %w", format, err)
	}
	errFn := func(err error) {
		v.logger.Error(err, "format parse error", "format", string(format))
	}
	out, err := misccodec.ParseFunc(format, errFn)(z)
	if err != nil {
		return nil, fmt.Errorf("format.%s: %w", format, err)
	}
	entries, err := v.toJSEntries(out)
	if err != nil {
		return nil, fmt.Errorf("format.%s: encode result: %w", format, err)
	}
	return v.rt.ToValue(entries), nil
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
