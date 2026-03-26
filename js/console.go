package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/dop251/goja"
)

func (v *VM) injectConsole() error {
	consoleObj := v.rt.NewObject()
	if err := consoleObj.Set("log", v.wrap(v.consoleLog)); err != nil {
		return err
	}
	if err := v.rt.Set("console", consoleObj); err != nil {
		return err
	}
	return nil
}

func (v *VM) consoleLog(call goja.FunctionCall) (goja.Value, error) {
	parts := make([]string, 0, len(call.Arguments))
	for _, arg := range call.Arguments {
		parts = append(parts, v.formatConsoleValue(arg))
	}

	if len(parts) == 0 {
		fmt.Fprintln(os.Stdout)
		return goja.Undefined(), nil
	}

	fmt.Fprintln(os.Stdout, strings.Join(parts, " "))
	return goja.Undefined(), nil
}

func (v *VM) formatConsoleValue(arg goja.Value) string {
	if goja.IsUndefined(arg) {
		return "undefined"
	}
	if goja.IsNull(arg) {
		return "null"
	}

	exported := arg.Export()
	switch t := exported.(type) {
	case string:
		return t
	case bool:
		return fmt.Sprint(t)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(t)
	}

	jsonObj := v.rt.Get("JSON")
	if !goja.IsUndefined(jsonObj) && !goja.IsNull(jsonObj) {
		stringifyVal := jsonObj.ToObject(v.rt).Get("stringify")
		if stringify, ok := goja.AssertFunction(stringifyVal); ok {
			if s, err := stringify(goja.Undefined(), arg); err == nil {
				if !goja.IsUndefined(s) && !goja.IsNull(s) {
					return s.String()
				}
			}
		}
	}

	if b, err := json.Marshal(exported); err == nil {
		return string(b)
	}

	return arg.String()
}
