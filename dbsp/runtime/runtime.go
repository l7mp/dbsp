package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/l7mp/dbsp/dbsp/zset"
)

var (
	// ErrRuntimeMissingCircuit indicates that an input was received without any circuit.
	ErrRuntimeMissingCircuit = errors.New("runtime missing circuit")
	// ErrRuntimeNilCircuit indicates that Add received a nil circuit.
	ErrRuntimeNilCircuit = errors.New("runtime cannot add nil circuit")
)

// Config configures a Runtime instance.
type Config struct {
	Manager   Manager
	Producers []Producer
	Consumers []Consumer
	Circuits  []*Circuit
	Circuit   *Circuit
}

// Runtime executes one circuit with producers and consumers.
//
// Producers trigger circuit execution in their own goroutine context via the
// registered input handler. Circuit execution is serialized to protect circuit
// state.
type Runtime struct {
	manager Manager

	mu        sync.Mutex
	circuits  []*Circuit
	producers []Producer
	consumers []Consumer

	execMu sync.Mutex
}

var _ Runnable = (*Runtime)(nil)

// NewRuntime creates a Runtime.
func NewRuntime(cfg Config) *Runtime {
	m := cfg.Manager
	if m == nil {
		m = NewManager()
	}

	return &Runtime{
		manager:   m,
		circuits:  initCircuits(cfg.Circuit, cfg.Circuits),
		producers: append([]Producer(nil), cfg.Producers...),
		consumers: append([]Consumer(nil), cfg.Consumers...),
	}
}

// Add installs an additional runtime circuit.
func (r *Runtime) Add(c *Circuit) error {
	if c == nil {
		return ErrRuntimeNilCircuit
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.circuits = append(r.circuits, c)
	return nil
}

// Start runs producers and consumers under the associated manager.
func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	hasCircuit := len(r.circuits) > 0
	producers := append([]Producer(nil), r.producers...)
	consumers := append([]Consumer(nil), r.consumers...)
	r.mu.Unlock()

	if !hasCircuit {
		return ErrRuntimeMissingCircuit
	}

	handler := func(ctx context.Context, in Input) error {
		return r.handleInput(ctx, consumers, in)
	}

	for _, p := range producers {
		p.SetInputHandler(handler)
	}

	for _, p := range producers {
		if err := r.manager.Add(p); err != nil {
			return fmt.Errorf("runtime add producer: %w", err)
		}
	}
	for _, c := range consumers {
		if err := r.manager.Add(c); err != nil {
			return fmt.Errorf("runtime add consumer: %w", err)
		}
	}

	return r.manager.Start(ctx)
}

func (r *Runtime) handleInput(ctx context.Context, consumers []Consumer, in Input) error {
	r.execMu.Lock()
	defer r.execMu.Unlock()

	r.mu.Lock()
	circuits := append([]*Circuit(nil), r.circuits...)
	r.mu.Unlock()

	if len(circuits) == 0 {
		return ErrRuntimeMissingCircuit
	}

	agg := map[string]zset.ZSet{}
	for _, c := range circuits {
		outs, err := c.Execute(in)
		if err != nil {
			return err
		}
		for _, out := range outs {
			if current, ok := agg[out.Name]; ok {
				agg[out.Name] = current.Add(out.Data)
			} else {
				agg[out.Name] = out.Data.Clone()
			}
		}
	}

	names := make([]string, 0, len(agg))
	for name := range agg {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		out := Output{Name: name, Data: agg[name]}
		for i, consumer := range consumers {
			payload := out
			if i < len(consumers)-1 {
				payload = Output{Name: out.Name, Data: out.Data.Clone()}
			}
			if err := consumer.Consume(ctx, payload); err != nil {
				return fmt.Errorf("runtime consume output %q: %w", out.Name, err)
			}
		}
	}

	return nil
}

func initCircuits(primary *Circuit, list []*Circuit) []*Circuit {
	circuits := make([]*Circuit, 0, len(list)+1)
	if primary != nil {
		circuits = append(circuits, primary)
	}
	for _, c := range list {
		if c != nil {
			circuits = append(circuits, c)
		}
	}
	return circuits
}
