package circuit

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
