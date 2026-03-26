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

	inputs := []compileragg.Binding{{Name: "input", Logical: "input"}}
	output := compileragg.Binding{Name: "output", Logical: "output"}

	if len(call.Arguments) > 1 {
		var opts struct {
			Inputs any `json:"inputs"`
			Output any `json:"output"`
		}
		if err := decodeOptionValue(call.Argument(1), &opts); err != nil {
			return nil, fmt.Errorf("aggregate.compile options: %w", err)
		}
		if parsedInputs, err := parseInputBindings(opts.Inputs); err != nil {
			return nil, fmt.Errorf("aggregate.compile options.inputs: %w", err)
		} else if len(parsedInputs) > 0 {
			inputs = parsedInputs
		}
		if parsedOutput, err := parseOutputBinding(opts.Output); err != nil {
			return nil, fmt.Errorf("aggregate.compile options.output: %w", err)
		} else if parsedOutput.Name != "" {
			output = parsedOutput
		}
	}

	compiler := compileragg.New(inputs, []compileragg.Binding{output})
	compiled, err := compiler.CompileString(string(pipelineJSON))
	if err != nil {
		return nil, fmt.Errorf("aggregate.compile: %w", err)
	}

	h := &circuitHandle{c: compiled.Circuit, query: compiled, vm: v}
	if err := h.register(); err != nil {
		return nil, fmt.Errorf("aggregate.compile: %w", err)
	}
	return h.jsObject(), nil
}

func parseInputBindings(raw any) ([]compileragg.Binding, error) {
	if raw == nil {
		return nil, nil
	}

	switch in := raw.(type) {
	case string:
		if in == "" {
			return nil, nil
		}
		return []compileragg.Binding{{Name: in, Logical: in}}, nil
	case []any:
		inputs := make([]compileragg.Binding, 0, len(in))
		for i, item := range in {
			b, err := parseInputBinding(item)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			if b.Name == "" {
				continue
			}
			inputs = append(inputs, b)
		}
		return inputs, nil
	case map[string]any:
		b, err := parseInputBinding(in)
		if err != nil {
			return nil, err
		}
		if b.Name == "" {
			return nil, nil
		}
		return []compileragg.Binding{b}, nil
	default:
		return nil, fmt.Errorf("must be a string, binding object, or array")
	}
}

func parseInputBinding(raw any) (compileragg.Binding, error) {
	b, err := parseBinding(raw, "", false)
	if err != nil {
		return compileragg.Binding{}, err
	}
	if b.Logical == "" {
		b.Logical = b.Name
	}
	return b, nil
}

func parseOutputBinding(raw any) (compileragg.Binding, error) {
	if raw == nil {
		return compileragg.Binding{}, nil
	}
	b, err := parseBinding(raw, "output", true)
	if err != nil {
		return compileragg.Binding{}, err
	}
	if b.Name == "" {
		return compileragg.Binding{}, nil
	}
	if b.Logical == "" {
		b.Logical = "output"
	}
	return b, nil
}

func parseBinding(raw any, defaultLogical string, stringUsesDefaultLogical bool) (compileragg.Binding, error) {
	switch v := raw.(type) {
	case string:
		if v == "" {
			return compileragg.Binding{}, nil
		}
		logical := v
		if stringUsesDefaultLogical && defaultLogical != "" {
			logical = defaultLogical
		}
		return compileragg.Binding{Name: v, Logical: logical}, nil
	case map[string]any:
		name := ""
		if x, ok := v["name"]; ok {
			if s, ok := x.(string); ok {
				name = s
			} else {
				return compileragg.Binding{}, fmt.Errorf("field 'name' must be a string")
			}
		}
		logical := defaultLogical
		if x, ok := v["logicalName"]; ok {
			s, ok := x.(string)
			if !ok {
				return compileragg.Binding{}, fmt.Errorf("field 'logicalName' must be a string")
			}
			logical = s
		}
		if x, ok := v["logical"]; ok {
			s, ok := x.(string)
			if !ok {
				return compileragg.Binding{}, fmt.Errorf("field 'logical' must be a string")
			}
			logical = s
		}
		return compileragg.Binding{Name: name, Logical: logical}, nil
	default:
		return compileragg.Binding{}, fmt.Errorf("must be a string or binding object")
	}
}
