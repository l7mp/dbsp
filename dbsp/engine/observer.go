package engine

import (
	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/zset"
)

// ExecutionSnapshot captures the state of circuit execution at a point in time.
type ExecutionSnapshot struct {
	// Values maps node IDs to their computed Z-set values.
	Values map[string]zset.ZSet

	// Schedule is the full topological execution order.
	Schedule []string

	// Position is the current index in the schedule (the node just evaluated).
	Position int
}

// Observer receives callbacks during circuit execution for debugging and
// introspection.
type Observer interface {
	// OnNodeEvaluated is called after a node has been evaluated.
	OnNodeEvaluated(node *circuit.Node, snapshot *ExecutionSnapshot)
}
