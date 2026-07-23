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
	"github.com/l7mp/dbsp/engine/datamodel"
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

	// A topic that is both an input and an output of the same circuit
	// short-circuits any control loop through the bus: Publish delivers to
	// every subscriber of the topic, so the circuit's own emissions come
	// back as observations. A controlled loop closes through the plant, on
	// distinct observed and command topics.
	for _, out := range outputNames {
		if _, ok := inputMap[out]; ok {
			logger.Info("warning: circuit subscribes to and publishes the same topic; its emissions will be re-ingested as observations",
				"circuit", name, "topic", out)
		}
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

// maxStepEntries bounds how many Z-set entries a single step may fold from
// the event backlog: draining stops once the running total reaches the cap
// and the remaining events wait for the next step.
const maxStepEntries = 128

// Start subscribes to all query inputs and forwards outputs via Publisher.
// Execute and publish errors are non-critical: they are reported via the
// runtime error channel and the circuit continues processing subsequent events.
// Start only returns a non-nil error on context cancellation-related issues.
//
// Each step folds the entire queued backlog into one execution: deltas for
// the same input topic add up as Z-sets (opposite weights cancel) and
// distinct inputs step together. Incremental circuits are correct for any
// per-step input delta, and ∫∘C^Δ = C∘∫ preserves the cumulative output
// (but only cumulative output!).
func (c *Circuit) Start(ctx context.Context) error {
	stop := context.AfterFunc(ctx, c.Subscriber.UnsubscribeAll)
	defer stop()

	for {
		in, ok := c.Subscriber.Next()
		if !ok {
			return nil
		}

		inputs := map[string]zset.ZSet{}
		total := c.foldInput(inputs, in, 0)
		for n := c.Subscriber.QueueSize(); n > 0 && total < maxStepEntries; n-- {
			ev, ok := c.Subscriber.Next()
			if !ok {
				return nil
			}
			total = c.foldInput(inputs, ev, total)
		}
		if len(inputs) == 0 {
			continue
		}

		outs, err := c.Execute(inputs)
		if err != nil {
			c.rt.ReportError(c.name, err)
			continue
		}
		for _, out := range outs {
			// An empty delta carries no information: suppress it instead of
			// waking every subscriber of the output topic.
			if out.Data.IsZero() {
				continue
			}
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

// foldInput folds one received event into the per-input step deltas and
// returns the updated entry total. Events for unknown topics are skipped.
// The first event per input is shallow-cloned before folding: event payloads
// are shared with every other subscriber of the topic and must never be
// mutated.
func (c *Circuit) foldInput(inputs map[string]zset.ZSet, in Event, total int) int {
	logical, ok := c.topicToInput[in.Name]
	if !ok {
		return total
	}
	if acc, ok := inputs[logical]; ok {
		in.Data.Iter(func(doc datamodel.Document, w zset.Weight) bool {
			acc.Insert(doc, w)
			return true
		})
	} else {
		inputs[logical] = in.Data.ShallowCopy()
	}
	return total + in.Data.Size()
}

// Execute applies one step to the compiled circuit. inputs maps logical
// input names to their deltas; absent inputs step with the empty Z-set.
// Execute takes ownership of the passed Z-sets.
func (c *Circuit) Execute(inputs map[string]zset.ZSet) ([]Event, error) {
	result, err := c.exec.ExecuteWithObserver(c.buildStepInputs(inputs), c.getObserver())
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

func (c *Circuit) buildStepInputs(folded map[string]zset.ZSet) map[string]zset.ZSet {
	inputs := make(map[string]zset.ZSet, len(c.inputMap))
	for _, logical := range c.inputNames {
		inputs[c.inputMap[logical]] = zset.New()
	}
	for logical, delta := range folded {
		inputs[c.inputMap[logical]] = delta
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
