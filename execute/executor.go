// Package execute provides circuit execution for DBSP.
package execute

import (
	"fmt"
	"sort"

	"github.com/l7mp/dbsp/circuit"
	"github.com/l7mp/dbsp/zset"
)

// State holds the stateful components of circuit execution.
type State struct {
	Delays          map[string]zset.ZSet
	Integrators     map[string]zset.ZSet
	Differentiators map[string]zset.ZSet
	Delta0Fired     map[string]bool
}

// NewState creates a new empty state.
func NewState() *State {
	return &State{
		Delays:          make(map[string]zset.ZSet),
		Integrators:     make(map[string]zset.ZSet),
		Differentiators: make(map[string]zset.ZSet),
		Delta0Fired:     make(map[string]bool),
	}
}

// Clone creates a deep copy of the state.
func (s *State) Clone() *State {
	clone := NewState()
	for k, v := range s.Delays {
		clone.Delays[k] = v.Clone()
	}
	for k, v := range s.Integrators {
		clone.Integrators[k] = v.Clone()
	}
	for k, v := range s.Differentiators {
		clone.Differentiators[k] = v.Clone()
	}
	for k, v := range s.Delta0Fired {
		clone.Delta0Fired[k] = v
	}
	return clone
}

// Executor executes a circuit.
type Executor struct {
	circuit  *circuit.Circuit
	schedule []string
	state    *State
}

// NewExecutor creates a new executor for the given circuit.
func NewExecutor(c *circuit.Circuit) (*Executor, error) {
	if errs := c.Validate(); len(errs) > 0 {
		return nil, fmt.Errorf("invalid circuit: %v", errs)
	}

	schedule, err := computeSchedule(c)
	if err != nil {
		return nil, err
	}

	return &Executor{
		circuit:  c,
		schedule: schedule,
		state:    NewState(),
	}, nil
}

// Execute runs one step of the circuit with the given inputs.
func (e *Executor) Execute(inputs map[string]zset.ZSet) (map[string]zset.ZSet, error) {
	values := make(map[string]zset.ZSet)

	// Set input values.
	for id, zs := range inputs {
		values[id] = zs
	}

	// Phase 1: Set delay outputs to their stored (previous) values.
	// This makes delay outputs available before their inputs are computed.
	for _, nodeID := range e.schedule {
		node := e.circuit.Node(nodeID)
		if node.Kind == circuit.NodeDelay {
			prev, exists := e.state.Delays[nodeID]
			if !exists {
				prev = zset.New()
			}
			values[nodeID] = prev
		}
	}

	// Phase 2: Process all non-delay nodes in schedule order.
	for _, nodeID := range e.schedule {
		node := e.circuit.Node(nodeID)

		switch node.Kind {
		case circuit.NodeInput:
			if _, exists := values[nodeID]; !exists {
				values[nodeID] = zset.New()
			}

		case circuit.NodeOutput:
			values[nodeID] = e.getInput(nodeID, values, 0)

		case circuit.NodeOperator:
			inEdges := e.circuit.EdgesTo(nodeID)
			opInputs := make([]zset.ZSet, node.Operator.Arity())
			for _, edge := range inEdges {
				opInputs[edge.Port] = values[edge.From]
			}
			// Fill in any missing inputs with empty Z-sets.
			for i := range opInputs {
				if opInputs[i].Size() == 0 && !opInputs[i].IsZero() {
					opInputs[i] = zset.New()
				}
			}

			result, err := node.Operator.Apply(opInputs...)
			if err != nil {
				return nil, fmt.Errorf("node %s: %w", nodeID, err)
			}
			values[nodeID] = result

		case circuit.NodeDelay:
			// Already handled in phase 1.
			// Will update storage in phase 3.

		case circuit.NodeIntegrate:
			// Running sum: output = accumulator + input.
			in := e.getInput(nodeID, values, 0)
			acc, exists := e.state.Integrators[nodeID]
			if !exists {
				acc = zset.New()
			}
			acc = acc.Add(in)
			e.state.Integrators[nodeID] = acc
			values[nodeID] = acc

		case circuit.NodeDifferentiate:
			// Difference from previous: output = current - previous.
			in := e.getInput(nodeID, values, 0)
			prev, exists := e.state.Differentiators[nodeID]
			if !exists {
				prev = zset.New()
			}
			values[nodeID] = in.Subtract(prev)
			e.state.Differentiators[nodeID] = in.Clone()

		case circuit.NodeDelta0:
			// Output input on first call, empty thereafter.
			if e.state.Delta0Fired[nodeID] {
				values[nodeID] = zset.New()
			} else {
				values[nodeID] = e.getInput(nodeID, values, 0)
				e.state.Delta0Fired[nodeID] = true
			}
		}
	}

	// Phase 3: Update delay storage with their inputs (for next step).
	for _, nodeID := range e.schedule {
		node := e.circuit.Node(nodeID)
		if node.Kind == circuit.NodeDelay {
			in := e.getInput(nodeID, values, 0)
			e.state.Delays[nodeID] = in
		}
	}

	// Collect outputs.
	outputs := make(map[string]zset.ZSet)
	for _, node := range e.circuit.Outputs() {
		outputs[node.ID] = values[node.ID]
	}

	return outputs, nil
}

// getInput returns the input value at the given port for a node.
func (e *Executor) getInput(nodeID string, values map[string]zset.ZSet, port int) zset.ZSet {
	for _, edge := range e.circuit.EdgesTo(nodeID) {
		if edge.Port == port {
			if v, exists := values[edge.From]; exists {
				return v
			}
		}
	}
	return zset.New()
}

// Reset clears all state.
func (e *Executor) Reset() {
	e.state = NewState()
}

// State returns a copy of the current state.
func (e *Executor) State() *State {
	return e.state.Clone()
}

// computeSchedule computes a topological order for execution.
// Delay nodes are processed first (they output previous values), then the rest in topological order.
func computeSchedule(c *circuit.Circuit) ([]string, error) {
	var result []string

	// Phase 1: Add all delay nodes first.
	// They output their stored values from the previous timestep.
	var delayNodes []string
	for _, n := range c.Nodes() {
		if n.Kind == circuit.NodeDelay {
			delayNodes = append(delayNodes, n.ID)
		}
	}
	sort.Strings(delayNodes)
	result = append(result, delayNodes...)

	// Phase 2: Topological sort for non-delay nodes.
	// Edges FROM delay nodes count as satisfied (they were processed in phase 1).
	inDegree := make(map[string]int)
	for _, n := range c.Nodes() {
		if n.Kind != circuit.NodeDelay {
			inDegree[n.ID] = 0
		}
	}

	for _, e := range c.Edges() {
		// Skip edges involving delay nodes (handled separately).
		if c.Node(e.To).Kind == circuit.NodeDelay {
			continue
		}
		// Edges from delay nodes are already satisfied.
		if c.Node(e.From).Kind == circuit.NodeDelay {
			continue
		}
		inDegree[e.To]++
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		result = append(result, id)

		for _, e := range c.EdgesFrom(id) {
			if c.Node(e.To).Kind == circuit.NodeDelay {
				continue
			}
			inDegree[e.To]--
			if inDegree[e.To] == 0 {
				queue = append(queue, e.To)
			}
		}
		sort.Strings(queue)
	}

	return result, nil
}
