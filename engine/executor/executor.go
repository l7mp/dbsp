package executor

import (
	"fmt"
	"maps"
	"sort"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
	"github.com/l7mp/dbsp/engine/internal/logger"
)

// Executor executes a circuit.
type Executor struct {
	circuit  *circuit.Circuit
	schedule []string
	logger   logr.Logger
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

		result, err := node.Operator.Apply(opInputs...)
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
