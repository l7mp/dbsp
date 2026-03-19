package runtime

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/executor"
	"github.com/l7mp/dbsp/dbsp/transform"
	"github.com/l7mp/dbsp/dbsp/zset"
)

var (
	// ErrUnknownInput indicates that an input name is not part of the runtime input map.
	ErrUnknownInput = errors.New("runtime unknown input")
	// ErrMissingOutput indicates that an expected output is missing from executor results.
	ErrMissingOutput = errors.New("runtime missing output")
)

// DefaultOutputBufferSize is the output channel capacity for Circuit.
const DefaultOutputBufferSize = 16

// CircuitConfig configures a Circuit runtime endpoint.
type CircuitConfig struct {
	Circuit     *circuit.Circuit
	InputMap    map[string]string
	OutputMap   map[string]string
	Incremental bool
	Logger      logr.Logger
}

// Circuit is an endpoint that executes a compiled DBSP circuit.
//
// Each received input triggers one executor step. The caller is responsible for
// feeding input semantics (delta/snapshot) that match Incremental.
type Circuit struct {
	circuit     *circuit.Circuit
	inputMap    map[string]string
	outputMap   map[string]string
	incremental bool

	exec *executor.Executor
	in   chan Input
	out  chan Output

	inputNames  []string
	outputNames []string
	state       map[string]zset.ZSet
}

var _ Processor = (*Circuit)(nil)

// NewCircuit creates a runtime endpoint for a compiled circuit.
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
		circuit:     compiled,
		inputMap:    inputMap,
		outputMap:   outputMap,
		incremental: cfg.Incremental,
		exec:        exec,
		in:          make(chan Input),
		out:         make(chan Output, DefaultOutputBufferSize),
		inputNames:  inputNames,
		outputNames: outputNames,
		state:       state,
	}, nil
}

// Input returns the runtime input channel.
func (r *Circuit) Input() chan<- Input {
	return r.in
}

// Output returns the runtime output channel.
func (r *Circuit) Output() <-chan Output {
	return r.out
}

// Start runs the immediate-trigger event loop.
func (r *Circuit) Start(ctx context.Context) error {
	defer close(r.out)
	defer r.exec.Reset()

	for {
		select {
		case <-ctx.Done():
			return nil
		case in, ok := <-r.in:
			if !ok {
				return nil
			}

			if _, ok := r.inputMap[in.Name]; !ok {
				return fmt.Errorf("%w: %s", ErrUnknownInput, in.Name)
			}

			stepInputs := r.buildStepInputs(in)
			result, err := r.exec.Execute(stepInputs)
			if err != nil {
				return fmt.Errorf("runtime step: %w", err)
			}

			for _, logical := range r.outputNames {
				nodeID := r.outputMap[logical]
				data, ok := result[nodeID]
				if !ok {
					return fmt.Errorf("%w: %s", ErrMissingOutput, logical)
				}
				if err := r.emit(ctx, Output{Name: logical, Data: data}); err != nil {
					return err
				}
			}
		}
	}
}

func (r *Circuit) buildStepInputs(in Input) map[string]zset.ZSet {
	inputs := make(map[string]zset.ZSet, len(r.inputMap))

	if r.incremental {
		for _, logical := range r.inputNames {
			nodeID := r.inputMap[logical]
			inputs[nodeID] = zset.New()
		}
		inputs[r.inputMap[in.Name]] = in.Data.Clone()
	} else {
		r.state[in.Name] = in.Data.Clone()
		for _, logical := range r.inputNames {
			nodeID := r.inputMap[logical]
			inputs[nodeID] = r.state[logical].Clone()
		}
	}

	return inputs
}

func (r *Circuit) emit(ctx context.Context, out Output) error {
	select {
	case <-ctx.Done():
		return nil
	case r.out <- out:
		return nil
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
