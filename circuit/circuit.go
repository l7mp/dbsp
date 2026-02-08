package circuit

import (
	"fmt"

	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"
)

// Circuit is a directed graph of nodes and edges.
type Circuit struct {
	name     string
	nodes    map[string]*Node
	edges    []*Edge
	graph    *simple.DirectedGraph
	nodeToID map[string]int64
	idToNode map[int64]string
}

// New creates a new empty circuit.
func New(name string) *Circuit {
	return &Circuit{
		name:     name,
		nodes:    make(map[string]*Node),
		edges:    make([]*Edge, 0),
		graph:    simple.NewDirectedGraph(),
		nodeToID: make(map[string]int64),
		idToNode: make(map[int64]string),
	}
}

// Name returns the circuit's name.
func (c *Circuit) Name() string { return c.name }

// AddNode adds a node to the circuit.
func (c *Circuit) AddNode(n *Node) error {
	if _, exists := c.nodes[n.ID]; exists {
		return fmt.Errorf("node %s already exists", n.ID)
	}
	c.nodes[n.ID] = n

	gn := c.graph.NewNode()
	c.graph.AddNode(gn)
	c.nodeToID[n.ID] = gn.ID()
	c.idToNode[gn.ID()] = n.ID

	return nil
}

// AddEdge adds an edge to the circuit.
func (c *Circuit) AddEdge(e *Edge) error {
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

// Inputs returns all input nodes.
func (c *Circuit) Inputs() []*Node {
	var result []*Node
	for _, n := range c.nodes {
		if n.Kind == NodeInput {
			result = append(result, n)
		}
	}
	return result
}

// Outputs returns all output nodes.
func (c *Circuit) Outputs() []*Node {
	var result []*Node
	for _, n := range c.nodes {
		if n.Kind == NodeOutput {
			result = append(result, n)
		}
	}
	return result
}

// Clone creates a copy of the circuit.
func (c *Circuit) Clone() *Circuit {
	clone := New(c.name)
	for _, n := range c.nodes {
		clone.AddNode(&Node{ID: n.ID, Kind: n.Kind, Operator: n.Operator})
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

// Validate checks if the circuit is well-formed.
// A circuit is well-formed if every cycle contains at least one delay node.
func (c *Circuit) Validate() []error {
	var errs []error
	for _, scc := range c.FindSCCs() {
		if len(scc) > 1 {
			hasDelay := false
			for _, id := range scc {
				if c.nodes[id].Kind == NodeDelay {
					hasDelay = true
					break
				}
			}
			if !hasDelay {
				errs = append(errs, fmt.Errorf("cycle %v has no delay", scc))
			}
		}
	}
	return errs
}
