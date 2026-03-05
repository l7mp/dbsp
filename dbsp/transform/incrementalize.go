// Package transform provides circuit transformations for DBSP.
package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/operator"
)

func incrementalID(id string) string {
	return id + "^Δ"
}

// nodeMapping tracks how original nodes map to result nodes.
type nodeMapping struct {
	// outputNode is the node that should be used as the source for
	// edges that originally came from this node.
	outputNode string
	// skip is true if this node is bypassed (integrate/differentiate).
	skip bool
	// inputNode is the node that should receive edges originally going to this node.
	// For bypassed nodes, this is empty (edges go directly to outputs).
	inputNode string
}

// Incrementalize transforms circuit C into C^Δ (Algorithm 6.4 from DBSP spec).
//
// Rules:
// 1. Linear operators pass through unchanged (O^Δ = O).
// 2. Bilinear operators use the three-term expansion pattern.
// 3. Non-linear operators use D ∘ O ∘ ∫ pattern.
// 4. Primitive nodes have special rules:
//   - z⁻¹ passes through.
//   - ∫^Δ = identity (bypass).
//   - D^Δ = identity (bypass).
//   - δ₀^Δ = δ₀.
func Incrementalize(c *circuit.Circuit) (*circuit.Circuit, error) {
	result := circuit.New(fmt.Sprintf("%s^Δ", c.Name()))

	// Mapping from original node IDs to their transformation info.
	mapping := make(map[string]*nodeMapping)

	// First pass: create all nodes and build the mapping.
	for _, node := range c.Nodes() {
		inputNode, outputNode := node.Incrementalize(result)
		if inputNode == "" && outputNode == "" {
			mapping[node.ID] = &nodeMapping{skip: true}
		} else {
			mapping[node.ID] = &nodeMapping{inputNode: inputNode, outputNode: outputNode}
		}
	}

	// Second pass: create edges.
	for _, e := range c.Edges() {
		fromNode := c.Node(e.From)
		fromMapping := mapping[e.From]
		toMapping := mapping[e.To]

		// Handle bypassed source nodes by looking up their input.
		if fromMapping.skip {
			// Find what feeds into this bypassed node.
			inEdges := c.EdgesTo(e.From)
			for _, inE := range inEdges {
				actualFrom := resolveSource(c, mapping, inE.From)
				actualTo := resolveDest(c, mapping, e.To, e.Port)
				if actualTo != "" {
					result.AddEdge(circuit.NewEdge(actualFrom, actualTo, e.Port))
				}
			}
			continue
		}

		// Handle bypassed destination nodes.
		if toMapping.skip {
			// Edges to bypassed nodes are handled when processing the bypassed node's outputs.
			continue
		}

		// Normal case: create edge from source output to dest input.
		toNode := c.Node(e.To)

		// Handle edges involving operators specially.
		// If target is an operator, it handles incoming edge creation.
		// If source is an operator, it handles outgoing edge creation.
		// When both are operators, target takes precedence for bilinear operators
		// since they need special wiring of inputs.
		if isUserOp(toNode) {
			createOperatorEdges(c, result, toNode, e, mapping)
		}
		if isUserOp(fromNode) {
			// For bilinear targets, skip - already handled above.
			if isUserOp(toNode) && toNode.Operator != nil &&
				toNode.Operator.Linearity() == operator.Bilinear {
				// Bilinear incoming edges are fully handled by the target.
				continue
			}
			createOperatorEdges(c, result, fromNode, e, mapping)
		}
		if !isUserOp(fromNode) && !isUserOp(toNode) {
			// Simple nodes: direct edge.
			actualFrom := fromMapping.outputNode
			actualTo := toMapping.inputNode
			result.AddEdge(circuit.NewEdge(actualFrom, actualTo, e.Port))
		}
	}

	return result, nil
}

// isUserOp returns true if the node holds a user-defined operator (Linear,
// Bilinear, or NonLinear), as opposed to a primitive circuit node.
func isUserOp(n *circuit.Node) bool {
	l := n.Operator.Linearity()
	return l == operator.Linear || l == operator.Bilinear || l == operator.NonLinear
}

// resolveSource finds the actual output node for a given source.
func resolveSource(c *circuit.Circuit, mapping map[string]*nodeMapping, nodeID string) string {
	m := mapping[nodeID]
	if m.skip {
		// Recursively find the source.
		for _, e := range c.EdgesTo(nodeID) {
			return resolveSource(c, mapping, e.From)
		}
		return "" // Should not happen in well-formed circuits.
	}
	return m.outputNode
}

// resolveDest finds the actual input node for a given destination.
func resolveDest(c *circuit.Circuit, mapping map[string]*nodeMapping, nodeID string, port int) string {
	m := mapping[nodeID]
	if m.skip {
		// Find where this bypassed node outputs to.
		for _, e := range c.EdgesFrom(nodeID) {
			return resolveDest(c, mapping, e.To, e.Port)
		}
		return "" // Should not happen in well-formed circuits.
	}
	return m.inputNode
}

// createOperatorEdges creates the external edges for an operator.
func createOperatorEdges(c, result *circuit.Circuit, node *circuit.Node, e *circuit.Edge, mapping map[string]*nodeMapping) {
	op := node.Operator
	prefix := incrementalID(node.ID)

	switch op.Linearity() {
	case operator.Linear:
		// Simple: use the operator node directly.
		if e.From == node.ID {
			// Outgoing edge.
			toMapping := mapping[e.To]
			result.AddEdge(circuit.NewEdge(mapping[node.ID].outputNode, toMapping.inputNode, e.Port))
		} else if e.To == node.ID {
			// Incoming edge.
			fromMapping := mapping[e.From]
			result.AddEdge(circuit.NewEdge(fromMapping.outputNode, mapping[node.ID].inputNode, e.Port))
		}

	case operator.Bilinear:
		if e.To == node.ID {
			// Incoming edge to bilinear operator.
			fromMapping := mapping[e.From]
			actualFrom := fromMapping.outputNode

			intLeft := prefix + "_int_left"
			intRight := prefix + "_int_right"
			delayLeft := prefix + "_delay_left"
			delayRight := prefix + "_delay_right"
			term1 := prefix + "_t1"
			term2 := prefix + "_t2"
			term3 := prefix + "_t3"

			if e.Port == 0 {
				// Left input (Δa).
				// Connect to: integrator, term1 port 0, term3 port 0.
				result.AddEdge(circuit.NewEdge(actualFrom, intLeft, 0))
				result.AddEdge(circuit.NewEdge(actualFrom, term1, 0))
				result.AddEdge(circuit.NewEdge(actualFrom, term3, 0))
				// delayLeft (∫a[t-1]) -> term2 port 0.
				result.AddEdge(circuit.NewEdge(delayLeft, term2, 0))
			} else {
				// Right input (Δb).
				// Connect to: integrator, term2 port 1, term3 port 1.
				result.AddEdge(circuit.NewEdge(actualFrom, intRight, 0))
				result.AddEdge(circuit.NewEdge(actualFrom, term2, 1))
				result.AddEdge(circuit.NewEdge(actualFrom, term3, 1))
				// delayRight (∫b[t-1]) -> term1 port 1.
				result.AddEdge(circuit.NewEdge(delayRight, term1, 1))
			}
		} else if e.From == node.ID {
			// Outgoing edge from bilinear operator.
			sumAll := prefix + "_sum"
			toMapping := mapping[e.To]
			result.AddEdge(circuit.NewEdge(sumAll, toMapping.inputNode, e.Port))
		}

	case operator.NonLinear:
		if e.To == node.ID {
			// Incoming edge.
			fromMapping := mapping[e.From]
			toNode := mapping[node.ID].inputNode
			result.AddEdge(circuit.NewEdge(fromMapping.outputNode, toNode, e.Port))
		} else if e.From == node.ID {
			// Outgoing edge.
			sourceNode := mapping[node.ID].outputNode
			toMapping := mapping[e.To]
			result.AddEdge(circuit.NewEdge(sourceNode, toMapping.inputNode, e.Port))
		}
	}
}
