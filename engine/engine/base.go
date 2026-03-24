package engine

import (
	"fmt"
	"maps"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/internal/logger"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
)

// Option configures an Engine.
type Option func(*baseEngine)

// WithIncremental sets whether the engine runs in incremental mode.
func WithIncremental(v bool) Option {
	return func(e *baseEngine) {
		e.incremental = v
	}
}

// WithLogger sets a custom logger.
func WithLogger(l logr.Logger) Option {
	return func(e *baseEngine) {
		e.logger = l
	}
}

// WithObserver sets an optional execution observer.
func WithObserver(obs Observer) Option {
	return func(e *baseEngine) {
		e.observer = obs
	}
}

// New creates a new Engine backed by the given Compiler.
func New(c compiler.Compiler, opts ...Option) Engine {
	e := &baseEngine{
		compiler:    c,
		incremental: false,
		running:     false,
		logger:      logger.DiscardLogger(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// baseEngine implements Engine.
type baseEngine struct {
	compiler    compiler.Compiler
	query       *compiler.Query
	executor    *executor.Executor
	observer    Observer
	incremental bool
	running     bool
	logger      logr.Logger
}

// Compile compiles a query source and prepares the circuit.
func (e *baseEngine) Compile(source string) error {
	ir, err := e.compiler.ParseString(source)
	if err != nil {
		return err
	}
	query, err := e.compiler.Compile(ir)
	if err != nil {
		return err
	}
	if query == nil || query.Circuit == nil {
		return fmt.Errorf("compile returned nil circuit")
	}
	compiled := query
	if e.incremental {
		incr, err := transform.Incrementalize(query.Circuit)
		if err != nil {
			return err
		}
		compiled = &compiler.Query{
			Circuit:          incr,
			InputMap:         cloneMap(query.InputMap),
			InputLogicalMap:  cloneMap(query.InputLogicalMap),
			OutputMap:        cloneMap(query.OutputMap),
			OutputLogicalMap: cloneMap(query.OutputLogicalMap),
		}
	}
	e.query = compiled
	e.executor = nil
	e.running = false
	return nil
}

// Start prepares the engine for execution.
func (e *baseEngine) Start() error {
	if e.query == nil || e.query.Circuit == nil {
		return fmt.Errorf("engine not compiled")
	}
	if e.running {
		return nil
	}
	exec, err := executor.New(e.query.Circuit, e.logger)
	if err != nil {
		return err
	}
	e.executor = exec
	e.running = true
	return nil
}

// Step executes one timestep.
func (e *baseEngine) Step(inputs map[string]zset.ZSet) (map[string]zset.ZSet, error) {
	if !e.running || e.executor == nil || e.query == nil {
		return nil, fmt.Errorf("engine not started")
	}
	inputMap := make(map[string]zset.ZSet, len(inputs))
	for logical, value := range inputs {
		id, ok := e.query.InputMap[logical]
		if !ok {
			return nil, fmt.Errorf("unknown input: %s", logical)
		}
		inputMap[id] = value
	}
	outputs, err := e.executor.ExecuteWithObserver(inputMap, e.executorObserver())
	if err != nil {
		return nil, err
	}
	result := make(map[string]zset.ZSet, len(e.query.OutputMap))
	for logical, id := range e.query.OutputMap {
		value, ok := outputs[id]
		if !ok {
			return nil, fmt.Errorf("missing output: %s", logical)
		}
		result[logical] = value
	}
	return result, nil
}

// Reset clears all internal state.
func (e *baseEngine) Reset() error {
	if e.executor == nil {
		return nil
	}
	e.executor.Reset()
	return nil
}

// Sync resynchronizes an incremental engine after Reset.
func (e *baseEngine) Sync() error {
	if !e.incremental {
		return nil
	}
	return fmt.Errorf("sync not implemented")
}

// Stop shuts down the engine and releases resources.
func (e *baseEngine) Stop() error {
	if !e.running {
		return nil
	}
	e.running = false
	e.executor = nil
	return nil
}

// IsIncremental returns true if the engine operates in incremental mode.
func (e *baseEngine) IsIncremental() bool {
	return e.incremental
}

// CompiledQuery returns the compiled query.
func (e *baseEngine) CompiledQuery() *compiler.Query {
	return e.query
}

func cloneMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)
	return dst
}

func (e *baseEngine) executorObserver() executor.ObserverFunc {
	if e.observer == nil {
		return nil
	}
	return func(node *circuit.Node, values map[string]zset.ZSet, schedule []string, position int) {
		snapshot := &ExecutionSnapshot{
			Values:   values,
			Schedule: schedule,
			Position: position,
		}
		e.observer.OnNodeEvaluated(node, snapshot)
	}
}

var _ Engine = (*baseEngine)(nil)
