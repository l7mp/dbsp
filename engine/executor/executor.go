// Package executor runs DBSP circuits one step at a time.
//
// # Value-passing contract
//
// The executor and the operators share Z-set containers and the documents
// inside them by reference; correctness rests on the following rules, which
// every operator and every embedder must follow.
//
// Document level:
//
//  1. Documents stored in a Z-set are immutable. Nothing may modify a
//     document after it has been inserted anywhere: containers share
//     document pointers freely (zset.ShallowCopy copies entries, not
//     documents), so a single mutation would be visible in accumulators,
//     delay cells and published outputs alike.
//  2. The invariant is enforced at the expression boundary: operators that
//     evaluate user expressions over stored elements (Select, Project) do so
//     against a per-element deep copy (elem.Copy()), so even the mutating
//     expression operators (@set, @setsub) only ever touch a private copy.
//     Code that hands documents to a mutating consumer uses zset.DeepCopy.
//
// Container level (the mutator pays):
//
//  3. A container, once emitted, is immutable for the rest of the step.
//     Most operators emit freshly built containers, immutable forever
//     (stateful operators like GroupByIncremental and EquiJoinH keep their
//     state private and emit fresh result sets; OutputOp sums its inputs
//     into a fresh container, so even boundary consumers always receive
//     frozen values). The one exception is the integrator: IntegrateOp
//     folds its input into its accumulator in place at the start of Apply,
//     before emission, so dataflow order alone guarantees readers see the
//     folded value, and re-emits the same container next step. Its output
//     is therefore step-scoped: consumers must not retain it across steps,
//     and wiring ∫ into a plain reference-holding delay does NOT yield the
//     previous integral (the value advances under the delay). To read the
//     previous integral, delay the deltas BEFORE integrating (z⁻¹ then ∫;
//     the operators commute), as the incrementalizer wires it: the delay
//     then holds only a frozen delta and the accumulator never leaves the
//     integrator.
//  4. Consumers treat received containers as read-only and build fresh
//     outputs. Under rules 3-4, passing by reference, fanning one container
//     out to several consumers, and retaining it across steps (the delay
//     cell stores and re-emits the received reference) are all safe and
//     free.
//
// Boundary rules:
//
//  5. Input Z-sets passed to Execute are borrowed for the step and remain
//     caller-owned; anything retained from them must be copied (the runtime
//     shallow-clones bus events before injection, DifferentiateOp
//     shallow-clones the prev it keeps).
//  6. Outputs returned by Execute follow rule 3: they are frozen, and the
//     runtime may publish them to any number of subscribers by reference.
//  7. Observers receive live references in a shallow map snapshot; an
//     observer that retains values across steps must copy them.
//
// Under these rules incremental circuits run in O(delta) per step: both
// integrators fold in place and copy nothing.
package executor

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/internal/logger"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
)

// Executor executes a circuit.
type Executor struct {
	circuit  *circuit.Circuit
	schedule []string
	logger   logr.Logger
	round    uint64
}

// ObserverFunc receives callbacks during execution.
// Values is a snapshot of computed node values at the time of the callback.
type ObserverFunc func(node *circuit.Node, values map[string]zset.ZSet, schedule []string, position int)

// New creates a new executor for the given circuit.
func New(c *circuit.Circuit, log logr.Logger) (*Executor, error) {
	if errs := c.Validate(); len(errs) > 0 {
		return nil, fmt.Errorf("invalid circuit: %v", errs)
	}

	schedule, err := computeSchedule(c)
	if err != nil {
		return nil, err
	}

	log = logger.NormalizeLogger(log)

	return &Executor{
		circuit:  c,
		schedule: schedule,
		logger:   log.WithName("execute"),
	}, nil
}

// Execute runs one step of the circuit with the given inputs.
func (e *Executor) Execute(inputs map[string]zset.ZSet) (map[string]zset.ZSet, error) {
	return e.ExecuteWithObserver(inputs, nil)
}

// ExecuteWithObserver runs one step of the circuit with optional callbacks.
func (e *Executor) ExecuteWithObserver(inputs map[string]zset.ZSet, observer ObserverFunc) (map[string]zset.ZSet, error) {
	e.round++
	execCtx := &operator.ExecContext{
		RoundID: e.round,
		Now:     time.Now().UTC().Format(time.RFC3339),
	}

	// Inject inputs: set each input node's stored value before execution.
	for id, v := range inputs {
		node := e.circuit.Node(id)
		if node == nil {
			return nil, fmt.Errorf("input node %s not found", id)
		}
		node.Operator.Set(v)
	}

	if e.logger.V(1).Enabled() {
		e.logger.V(1).Info("execute start", "inputs", formatZSetMap(inputs))
	}

	// Execute all nodes in topological order.
	// With the two-node delay design the circuit is a DAG, so plain Kahn's
	// order is sufficient — no special delay phasing is needed.
	values := make(map[string]zset.ZSet)
	for position, nodeID := range e.schedule {
		node := e.circuit.Node(nodeID)

		// Collect inputs from incoming edges.
		inEdges := e.circuit.EdgesTo(nodeID)
		arity := node.Operator.Arity()
		if node.Operator.Kind() == operator.KindOutput {
			maxPort := -1
			for _, edge := range inEdges {
				if edge.Port > maxPort {
					maxPort = edge.Port
				}
			}
			if maxPort+1 > arity {
				arity = maxPort + 1
			}
		}
		opInputs := make([]zset.ZSet, arity)
		for i := range opInputs {
			opInputs[i] = zset.New()
		}
		for _, edge := range inEdges {
			if edge.Port < arity {
				if v, ok := values[edge.From]; ok {
					opInputs[edge.Port] = v
				}
			}
		}

		result, err := node.Operator.Apply(execCtx, opInputs...)
		if err != nil {
			e.logger.Error(err, "operator apply failed", "node", nodeID)
			return nil, fmt.Errorf("node %s: %w", nodeID, err)
		}
		values[nodeID] = result
		e.observe(observer, node, values, position)

		if e.logger.V(2).Enabled() {
			e.logger.V(2).Info("node value",
				"node", nodeID,
				"kind", node.Operator.Kind().String(),
				"op", node.Operator.String(),
				"inputs", formatInputs(opInputs),
				"value", result.String(),
			)
		}
	}

	// Collect outputs.
	outputs := make(map[string]zset.ZSet)
	for _, node := range e.circuit.Outputs() {
		outputs[node.ID] = values[node.ID]
	}

	if e.logger.V(1).Enabled() {
		e.logger.V(1).Info("execute end", "outputs", formatZSetMap(outputs))
	}
	if e.logger.V(0).Enabled() {
		e.logger.Info("execute", "circuit", formatCircuit(e.circuit, e.schedule), "result", formatZSetMap(outputs))
	}

	return outputs, nil
}

func (e *Executor) observe(observer ObserverFunc, node *circuit.Node, values map[string]zset.ZSet, position int) {
	if observer == nil {
		return
	}
	snapshotValues := maps.Clone(values)
	observer(node, snapshotValues, e.schedule, position)
}

// Reset resets all operator state to its zero value.
func (e *Executor) Reset() {
	for _, node := range e.circuit.Nodes() {
		operator.Reset(node.Operator)
	}
}

// computeSchedule computes a topological execution order using Kahn's algorithm.
// The circuit is a DAG (delay nodes are split into emit/absorb pairs), so no
// special delay handling is needed.
func computeSchedule(c *circuit.Circuit) ([]string, error) {
	inDegree := make(map[string]int)
	for _, n := range c.Nodes() {
		inDegree[n.ID] = 0
	}
	for _, e := range c.Edges() {
		inDegree[e.To]++
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	result := make([]string, 0, len(inDegree))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		result = append(result, id)

		for _, e := range c.EdgesFrom(id) {
			inDegree[e.To]--
			if inDegree[e.To] == 0 {
				queue = append(queue, e.To)
				sort.Strings(queue)
			}
		}
	}

	if len(result) != len(inDegree) {
		return nil, fmt.Errorf("circuit has a cycle; use delay nodes to break cycles")
	}

	return result, nil
}
