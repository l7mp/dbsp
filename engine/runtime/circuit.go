package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"sync"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ Processor = (*Circuit)(nil)

// Circuit is a runtime processor that subscribes to all query inputs and
// publishes all query outputs.
type Circuit struct {
	Publisher
	Subscriber

	name string
	rt   *Runtime

	inputMap    map[string]string
	outputMap   map[string]string
	inputNames  []string
	outputNames []string
	exec        *executor.Executor

	topicToInput  map[string]string
	docsFormatter func(Event) []string
	observer      executor.ObserverFunc
	observerMu    sync.RWMutex

	logger logr.Logger
}

// NewCircuit builds a runtime processor from a compiled query.
// name is a unique identifier for this circuit within the runtime; it is used
// as the origin field in any ComponentErrors reported by the circuit. Name
// uniqueness is enforced when the circuit is passed to Runtime.Add.
func NewCircuit(name string, rt *Runtime, q *compiler.Query, logger logr.Logger) (*Circuit, error) {
	exec, err := executor.New(q.Circuit, logr.Discard())
	if err != nil {
		return nil, fmt.Errorf("runtime executor: %w", err)
	}

	inputMap := maps.Clone(q.InputMap)
	outputMap := maps.Clone(q.OutputMap)
	inputNames := sortedKeys(inputMap)
	outputNames := sortedKeys(outputMap)

	c := &Circuit{
		Publisher:    rt.NewPublisher(),
		Subscriber:   rt.NewSubscriber(),
		name:         name,
		rt:           rt,
		inputMap:     inputMap,
		outputMap:    outputMap,
		inputNames:   inputNames,
		outputNames:  outputNames,
		exec:         exec,
		topicToInput: map[string]string{},
		logger:       logger,
	}

	// Pre-subscribe to input topics so events published right after runtime.Add
	// are not dropped before Start begins consuming.
	for _, in := range c.inputNames {
		c.Subscribe(in)
		c.topicToInput[in] = in
	}

	return c, nil
}

// Name returns the circuit's unique component name.
func (c *Circuit) Name() string { return c.name }

// String implements fmt.Stringer.
func (c *Circuit) String() string {
	if c == nil {
		return "processor<circuit>{<nil>}"
	}
	return fmt.Sprintf("processor<circuit>{name=%q, topics=%v, outputs=%v}", c.name, c.inputNames, c.outputNames)
}

// MarshalJSON provides a stable machine-readable representation.
func (c *Circuit) MarshalJSON() ([]byte, error) {
	if c == nil {
		return json.Marshal(map[string]any{"component": "processor", "type": "circuit", "nil": true})
	}

	return json.Marshal(map[string]any{
		"component": "processor",
		"type":      "circuit",
		"name":      c.name,
		"inputs":    append([]string(nil), c.inputNames...),
		"outputs":   append([]string(nil), c.outputNames...),
	})
}

// SetDocsFormatter overrides full-doc flow logging payloads.
func (c *Circuit) SetDocsFormatter(f func(Event) []string) {
	c.docsFormatter = f
}

// SetObserver installs an optional executor observer callback.
func (c *Circuit) SetObserver(observer executor.ObserverFunc) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	c.observer = observer
}

// Start subscribes to all query inputs and forwards outputs via Publisher.
// Execute and publish errors are non-critical: they are reported via the
// runtime error channel and the circuit continues processing subsequent events.
// Start only returns a non-nil error on context cancellation-related issues.
func (c *Circuit) Start(ctx context.Context) error {
	stop := context.AfterFunc(ctx, c.Subscriber.UnsubscribeAll)
	defer stop()

	for {
		in, ok := c.Subscriber.Next()
		if !ok {
			return nil
		}
		logical, ok := c.topicToInput[in.Name]
		if !ok {
			continue
		}
		in.Name = logical
		outs, err := c.Execute(in)
		if err != nil {
			c.rt.ReportError(c.name, err)
			continue
		}
		for _, out := range outs {
			var docs []string
			if c.docsFormatter != nil && c.logger.V(2).Enabled() {
				docs = c.docsFormatter(out)
			}
			LogFlowEvent(c.logger, "processor.send", "processor", c.String(), "output", out.Name, "", out.Data, docs)
			if err := c.Publish(out); err != nil {
				c.rt.ReportError(c.name, err)
			}
		}
	}
}

// Execute applies one runtime event to the compiled circuit.
func (c *Circuit) Execute(in Event) ([]Event, error) {
	result, err := c.exec.ExecuteWithObserver(c.buildStepInputs(in), c.getObserver())
	if err != nil {
		return nil, fmt.Errorf("runtime step: %w", err)
	}
	outs := make([]Event, 0, len(c.outputNames))
	for _, logical := range c.outputNames {
		nodeID := c.outputMap[logical]
		outs = append(outs, Event{Name: logical, Data: result[nodeID]})
	}
	return outs, nil
}

func (c *Circuit) getObserver() executor.ObserverFunc {
	c.observerMu.RLock()
	defer c.observerMu.RUnlock()
	return c.observer
}

// Reset clears executor state.
func (c *Circuit) Reset() {
	c.exec.Reset()
}

func (c *Circuit) buildStepInputs(in Event) map[string]zset.ZSet {
	inputs := make(map[string]zset.ZSet, len(c.inputMap))
	for _, logical := range c.inputNames {
		inputs[c.inputMap[logical]] = zset.New()
	}
	inputs[c.inputMap[in.Name]] = in.Data.Clone()
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
