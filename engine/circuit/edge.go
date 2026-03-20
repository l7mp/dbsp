package circuit

import "gonum.org/v1/gonum/graph/simple"

// Edge represents a directed edge with port number.
type Edge struct {
	From string // Source node ID.
	To   string // Target node ID.
	Port int    // Input port on target (for multi-input operators).
}

// NewEdge creates a new edge.
func NewEdge(from, to string, port int) *Edge {
	return &Edge{From: from, To: to, Port: port}
}

// Circuit is a directed graph of nodes and edges.
type Circuit struct {
	name         string
	nodes        map[string]*Node
	edges        []*Edge
	inputIDs     map[string]bool // IDs of circuit-input boundary nodes.
	outputIDs    map[string]bool // IDs of circuit-output boundary nodes.
	delayEmitIDs map[string]bool // IDs of delay emit nodes (absorb = emit+"_absorb").
	graph        *simple.DirectedGraph
	nodeToID     map[string]int64
	idToNode     map[int64]string
}
