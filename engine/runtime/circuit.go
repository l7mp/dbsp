package runtime

import (
	"errors"
	"fmt"
	"maps"
	"sort"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
)

var (
	// ErrUnknownInput indicates that an input name is not part of the runtime input map.
	ErrUnknownInput = errors.New("runtime unknown input")
	// ErrMissingOutput indicates that an expected output is missing from executor results.
	ErrMissingOutput = errors.New("runtime missing output")
)

// CircuitConfig configures a Circuit runtime endpoint.
type CircuitConfig struct {
	Circuit     *circuit.Circuit
	InputMap    map[string]string
	OutputMap   map[string]string
	Incremental bool
	Logger      logr.Logger
}

// Circuit wraps a compiled circuit and executes one step per input event.
type Circuit struct {
	inputMap    map[string]string
	outputMap   map[string]string
	incremental bool
	exec        *executor.Executor

	inputNames  []string
	outputNames []string
	state       map[string]zset.ZSet
}

// NewCircuit creates a circuit wrapper from an already-compiled circuit and
// input/output maps.
func NewCircuit(cfg CircuitConfig) (*Circuit, error) {
	if cfg.Circuit == nil {
		return nil, fmt.Errorf("runtime config: circuit is required")
	}
	if len(cfg.InputMap) == 0 {
		return nil, fmt.Errorf("runtime config: input map is required")
	}
	if len(cfg.OutputMap) == 0 {
		return nil, fmt.Errorf("runtime config: output map is required")
	}

	compiled := cfg.Circuit
	if cfg.Incremental {
		incr, err := transform.Incrementalize(cfg.Circuit)
		if err != nil {
			return nil, fmt.Errorf("runtime incrementalize: %w", err)
		}
		compiled = incr
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	exec, err := executor.New(compiled, log)
	if err != nil {
		return nil, fmt.Errorf("runtime executor: %w", err)
	}

	inputMap := maps.Clone(cfg.InputMap)
	outputMap := maps.Clone(cfg.OutputMap)

	inputNames := sortedKeys(inputMap)
	outputNames := sortedKeys(outputMap)

	state := make(map[string]zset.ZSet, len(inputNames))
	for _, name := range inputNames {
		state[name] = zset.New()
	}

	return &Circuit{
		inputMap:    inputMap,
		outputMap:   outputMap,
		incremental: cfg.Incremental,
		exec:        exec,
		inputNames:  inputNames,
		outputNames: outputNames,
		state:       state,
	}, nil
}

// Execute runs one circuit step and returns logical outputs.
func (c *Circuit) Execute(in Input) ([]Output, error) {
	if _, ok := c.inputMap[in.Name]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownInput, in.Name)
	}

	stepInputs := c.buildStepInputs(in)
	result, err := c.exec.Execute(stepInputs)
	if err != nil {
		return nil, fmt.Errorf("runtime step: %w", err)
	}

	outs := make([]Output, 0, len(c.outputNames))
	for _, logical := range c.outputNames {
		nodeID := c.outputMap[logical]
		data, ok := result[nodeID]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrMissingOutput, logical)
		}
		outs = append(outs, Output{Name: logical, Data: data})
	}

	return outs, nil
}

// Reset clears operator state and runtime snapshot cache.
func (c *Circuit) Reset() {
	c.exec.Reset()
	for _, name := range c.inputNames {
		c.state[name] = zset.New()
	}
}

func (c *Circuit) buildStepInputs(in Input) map[string]zset.ZSet {
	inputs := make(map[string]zset.ZSet, len(c.inputMap))

	if c.incremental {
		for _, logical := range c.inputNames {
			nodeID := c.inputMap[logical]
			inputs[nodeID] = zset.New()
		}
		inputs[c.inputMap[in.Name]] = in.Data.Clone()
	} else {
		c.state[in.Name] = in.Data.Clone()
		for _, logical := range c.inputNames {
			nodeID := c.inputMap[logical]
			inputs[nodeID] = c.state[logical].Clone()
		}
	}

	return inputs
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
