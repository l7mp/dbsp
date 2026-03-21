package main

import (
	"encoding/json"
	"fmt"

	"github.com/dop251/goja"

	compileragg "github.com/l7mp/dbsp/engine/compiler/aggregation"
)

func (v *VM) aggregateCompile(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("aggregate.compile(pipeline, { inputs, output }) requires pipeline")
	}

	pipelineJSON, err := json.Marshal(call.Argument(0).Export())
	if err != nil {
		return nil, fmt.Errorf("aggregate.compile pipeline: %w", err)
	}

	inputs := []string{"input"}
	output := "output"

	if len(call.Arguments) > 1 {
		var opts struct {
			Inputs any    `json:"inputs"`
			Output string `json:"output"`
		}
		if err := decodeOptionValue(call.Argument(1), &opts); err != nil {
			return nil, fmt.Errorf("aggregate.compile options: %w", err)
		}
		if parsedInputs, err := parseInputs(opts.Inputs); err != nil {
			return nil, fmt.Errorf("aggregate.compile options.inputs: %w", err)
		} else if len(parsedInputs) > 0 {
			inputs = parsedInputs
		}
		if opts.Output != "" {
			output = opts.Output
		}
	}

	compiler := compileragg.New(inputs, []string{"output"})
	compiled, err := compiler.CompileString(string(pipelineJSON))
	if err != nil {
		return nil, fmt.Errorf("aggregate.compile: %w", err)
	}

	outputMap, err := renameOutputMap(output, compiled.OutputMap)
	if err != nil {
		return nil, fmt.Errorf("aggregate.compile: %w", err)
	}
	compiled.OutputMap = outputMap

	h := &circuitHandle{c: compiled.Circuit, query: compiled, vm: v}
	return h.jsObject(), nil
}

func parseInputs(raw any) ([]string, error) {
	if raw == nil {
		return nil, nil
	}

	switch in := raw.(type) {
	case string:
		if in == "" {
			return nil, nil
		}
		return []string{in}, nil
	case []any:
		inputs := make([]string, 0, len(in))
		for i, item := range in {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("index %d must be a string", i)
			}
			inputs = append(inputs, s)
		}
		return inputs, nil
	default:
		return nil, fmt.Errorf("must be a string or array of strings")
	}
}
