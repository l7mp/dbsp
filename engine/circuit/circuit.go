package circuit

import (
	"fmt"
	"strings"

	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"

	"github.com/l7mp/dbsp/engine/operator"
)

// New creates a new empty circuit.
func New(name string) *Circuit {
	return &Circuit{
		name:         name,
		nodes:        make(map[string]*Node),
		edges:        make([]*Edge, 0),
		inputIDs:     make(map[string]bool),
		outputIDs:    make(map[string]bool),
		delayEmitIDs: make(map[string]bool),
		graph:        simple.NewDirectedGraph(),
		nodeToID:     make(map[string]int64),
		idToNode:     make(map[int64]string),
	}
}

// Name returns the circuit's name.
func (c *Circuit) Name() string { return c.name }

// AddNode adds a node to the circuit.
//
// Special behavior for delay nodes (KindDelay / *operator.DelayOp): the circuit
// automatically creates and registers the paired absorb node (id+"_absorb") so
// callers need only add the emit node. The emit ID is recorded in delayEmitIDs
// so that AddEdge can transparently rewrite edges that target the emit ID to
// target the absorb node instead.
func (c *Circuit) AddNode(n *Node) error {
	if _, exists := c.nodes[n.ID]; exists {
		return fmt.Errorf("node %s already exists", n.ID)
	}
	c.addNodeRaw(n)

	// Track circuit boundary nodes by operator kind.
	switch n.Kind() {
	case operator.KindInput:
		c.inputIDs[n.ID] = true
	case operator.KindOutput:
		c.outputIDs[n.ID] = true
	case operator.KindDelay:
		// Register the emit node and automatically create its absorb partner.
		// NewDelay returns a paired (emit, absorb) sharing the same internal state.
		c.delayEmitIDs[n.ID] = true
		emitOp, absorbOp := operator.NewDelay()
		n.Operator = emitOp
		c.addNodeRaw(&Node{
			ID:       n.ID + "_absorb",
			Operator: absorbOp,
		})
	}
	return nil
}

// addNodeRaw adds a node directly to the internal maps without any special processing.
// Used internally by AddNode and Clone.
func (c *Circuit) addNodeRaw(n *Node) {
	c.nodes[n.ID] = n
	gn := c.graph.NewNode()
	c.graph.AddNode(gn)
	c.nodeToID[n.ID] = gn.ID()
	c.idToNode[gn.ID()] = n.ID
}

// AddEdge adds an edge to the circuit.
//
// If the target node is a delay emit node (in delayEmitIDs), the edge is
// transparently rewritten to target the absorb node (id+"_absorb") instead,
// so callers use the same delay ID for both incoming and outgoing edges.
func (c *Circuit) AddEdge(e *Edge) error {
	// Rewrite edges that target a delay emit node to its absorb node.
	to := e.To
	if c.delayEmitIDs[to] {
		to = to + "_absorb"
		e = &Edge{From: e.From, To: to, Port: e.Port}
	}

	if _, exists := c.nodes[e.From]; !exists {
		return fmt.Errorf("source node %s not found", e.From)
	}
	if _, exists := c.nodes[e.To]; !exists {
		return fmt.Errorf("target node %s not found", e.To)
	}
	c.edges = append(c.edges, e)

	c.graph.SetEdge(c.graph.NewEdge(
		c.graph.Node(c.nodeToID[e.From]),
		c.graph.Node(c.nodeToID[e.To]),
	))

	return nil
}

// Node returns a node by ID.
func (c *Circuit) Node(id string) *Node {
	return c.nodes[id]
}

// Nodes returns all nodes.
func (c *Circuit) Nodes() []*Node {
	result := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		result = append(result, n)
	}
	return result
}

// Edges returns all edges.
func (c *Circuit) Edges() []*Edge {
	return c.edges
}

// EdgesTo returns all edges to a node.
func (c *Circuit) EdgesTo(nodeID string) []*Edge {
	var result []*Edge
	for _, e := range c.edges {
		if e.To == nodeID {
			result = append(result, e)
		}
	}
	return result
}

// EdgesFrom returns all edges from a node.
func (c *Circuit) EdgesFrom(nodeID string) []*Edge {
	var result []*Edge
	for _, e := range c.edges {
		if e.From == nodeID {
			result = append(result, e)
		}
	}
	return result
}

// Inputs returns all circuit-input boundary nodes.
func (c *Circuit) Inputs() []*Node {
	result := make([]*Node, 0, len(c.inputIDs))
	for id := range c.inputIDs {
		result = append(result, c.nodes[id])
	}
	return result
}

// Outputs returns all circuit-output boundary nodes.
func (c *Circuit) Outputs() []*Node {
	result := make([]*Node, 0, len(c.outputIDs))
	for id := range c.outputIDs {
		result = append(result, c.nodes[id])
	}
	return result
}

// Clone creates a structural copy of the circuit. Operator state is shared
// (not deep-copied), so the clone should be used for structural analysis,
// not concurrent execution.
func (c *Circuit) Clone() *Circuit {
	clone := &Circuit{
		name:         c.name,
		nodes:        make(map[string]*Node),
		edges:        make([]*Edge, 0, len(c.edges)),
		inputIDs:     make(map[string]bool, len(c.inputIDs)),
		outputIDs:    make(map[string]bool, len(c.outputIDs)),
		delayEmitIDs: make(map[string]bool, len(c.delayEmitIDs)),
		graph:        simple.NewDirectedGraph(),
		nodeToID:     make(map[string]int64),
		idToNode:     make(map[int64]string),
	}
	for id := range c.inputIDs {
		clone.inputIDs[id] = true
	}
	for id := range c.outputIDs {
		clone.outputIDs[id] = true
	}
	for id := range c.delayEmitIDs {
		clone.delayEmitIDs[id] = true
	}
	for _, n := range c.nodes {
		clone.addNodeRaw(&Node{ID: n.ID, Operator: n.Operator})
	}
	for _, e := range c.edges {
		clone.AddEdge(&Edge{From: e.From, To: e.To, Port: e.Port})
	}
	return clone
}

// FindSCCs returns all strongly connected components.
func (c *Circuit) FindSCCs() [][]string {
	sccs := topo.TarjanSCC(c.graph)
	result := make([][]string, len(sccs))
	for i, scc := range sccs {
		nodeIDs := make([]string, len(scc))
		for j, gn := range scc {
			nodeIDs[j] = c.idToNode[gn.ID()]
		}
		result[i] = nodeIDs
	}
	return result
}

// RemoveNode removes a node and all edges connected to it.
func (c *Circuit) RemoveNode(id string) error {
	if _, exists := c.nodes[id]; !exists {
		return fmt.Errorf("node %s not found", id)
	}

	// Remove all edges to/from this node.
	filtered := make([]*Edge, 0, len(c.edges))
	for _, e := range c.edges {
		if e.From != id && e.To != id {
			filtered = append(filtered, e)
		}
	}
	c.edges = filtered

	// Remove from gonum graph (also removes gonum edges).
	gonumID := c.nodeToID[id]
	c.graph.RemoveNode(gonumID)

	// Clean up maps.
	delete(c.nodes, id)
	delete(c.idToNode, gonumID)
	delete(c.nodeToID, id)
	delete(c.inputIDs, id)
	delete(c.outputIDs, id)
	delete(c.delayEmitIDs, id)

	return nil
}

// RemoveEdge removes a specific edge identified by from, to, and port.
func (c *Circuit) RemoveEdge(from, to string, port int) error {
	for i, e := range c.edges {
		if e.From == from && e.To == to && e.Port == port {
			c.edges = append(c.edges[:i], c.edges[i+1:]...)
			// Only remove the gonum edge if no other logical edges remain
			// between these two nodes (gonum does not support parallel edges).
			hasOther := false
			for _, e2 := range c.edges {
				if e2.From == from && e2.To == to {
					hasOther = true
					break
				}
			}
			if !hasOther {
				c.graph.RemoveEdge(c.nodeToID[from], c.nodeToID[to])
			}
			return nil
		}
	}
	return fmt.Errorf("edge %s -> %s (port %d) not found", from, to, port)
}

// BypassNode removes a node and reconnects its incoming edges to all of its
// outgoing edges. All incoming edges must originate from the same source node.
// The port numbers on outgoing edges are preserved.
func (c *Circuit) BypassNode(id string) error {
	if _, exists := c.nodes[id]; !exists {
		return fmt.Errorf("node %s not found", id)
	}

	inEdges := c.EdgesTo(id)
	if len(inEdges) == 0 {
		return fmt.Errorf("bypass requires at least 1 incoming edge, node %s has 0", id)
	}
	source := inEdges[0].From
	for _, e := range inEdges[1:] {
		if e.From != source {
			return fmt.Errorf("bypass requires all incoming edges from same source, node %s has edges from %s and %s", id, source, e.From)
		}
	}

	// Reconnect: all edges FROM this node now come FROM the source instead.
	outEdges := c.EdgesFrom(id)
	for _, e := range outEdges {
		e.From = source
		// Add new gonum edge from source to target.
		c.graph.SetEdge(c.graph.NewEdge(
			c.graph.Node(c.nodeToID[source]),
			c.graph.Node(c.nodeToID[e.To]),
		))
	}

	// Remove the incoming edge(s) and the node.
	filtered := make([]*Edge, 0, len(c.edges))
	for _, e := range c.edges {
		if e.To == id {
			continue
		}
		filtered = append(filtered, e)
	}
	c.edges = filtered

	// Remove node from gonum graph and maps.
	gonumID := c.nodeToID[id]
	c.graph.RemoveNode(gonumID)
	delete(c.nodes, id)
	delete(c.idToNode, gonumID)
	delete(c.nodeToID, id)
	delete(c.inputIDs, id)
	delete(c.outputIDs, id)
	delete(c.delayEmitIDs, id)

	return nil
}

// Validate checks that the circuit is a DAG (no cycles). A properly constructed
// circuit using delay nodes (which are split into emit/absorb pairs) will always
// be a DAG. Cycles indicate a missing delay.
func (c *Circuit) Validate() []error {
	var errs []error
	for _, scc := range c.FindSCCs() {
		if len(scc) > 1 {
			errs = append(errs, fmt.Errorf("circuit has a cycle %v; use delay nodes to break cycles", scc))
		}
	}
	return errs
}

// isDelayAbsorbID reports whether id is the absorb half of a delay pair.
func (c *Circuit) isDelayAbsorbID(id string) bool {
	if !strings.HasSuffix(id, "_absorb") {
		return false
	}
	emitID := strings.TrimSuffix(id, "_absorb")
	return c.delayEmitIDs[emitID]
}
