// Package transform provides circuit transformations for DBSP.
package transform

import (
	"fmt"

	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/operator"
)

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
		switch node.Kind {
		case circuit.NodeInput:
			result.AddNode(circuit.Input(node.ID))
			mapping[node.ID] = &nodeMapping{outputNode: node.ID, inputNode: node.ID}

		case circuit.NodeOutput:
			result.AddNode(circuit.Output(node.ID))
			mapping[node.ID] = &nodeMapping{outputNode: node.ID, inputNode: node.ID}

		case circuit.NodeOperator:
			m, err := createOperatorNodes(result, node)
			if err != nil {
				return nil, err
			}
			mapping[node.ID] = m

		case circuit.NodeIntegrate:
			// Bypassed - no node created.
			mapping[node.ID] = &nodeMapping{skip: true}

		case circuit.NodeDifferentiate:
			// Bypassed - no node created.
			mapping[node.ID] = &nodeMapping{skip: true}

		case circuit.NodeDelay:
			result.AddNode(circuit.Delay(node.ID))
			mapping[node.ID] = &nodeMapping{outputNode: node.ID, inputNode: node.ID}

		case circuit.NodeDelta0:
			result.AddNode(circuit.Delta0(node.ID))
			mapping[node.ID] = &nodeMapping{outputNode: node.ID, inputNode: node.ID}
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
		if toNode.Kind == circuit.NodeOperator {
			createOperatorEdges(c, result, toNode, e, mapping)
		}
		if fromNode.Kind == circuit.NodeOperator {
			// For bilinear targets, skip - already handled above.
			if toNode.Kind == circuit.NodeOperator && toNode.Operator != nil &&
				toNode.Operator.Linearity() == operator.Bilinear {
				// Bilinear incoming edges are fully handled by the target.
				continue
			}
			createOperatorEdges(c, result, fromNode, e, mapping)
		}
		if fromNode.Kind != circuit.NodeOperator && toNode.Kind != circuit.NodeOperator {
			// Simple nodes: direct edge.
			actualFrom := fromMapping.outputNode
			actualTo := toMapping.inputNode
			result.AddEdge(circuit.NewEdge(actualFrom, actualTo, e.Port))
		}
	}

	return result, nil
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

// createOperatorNodes creates the nodes for an operator in the incremental circuit.
func createOperatorNodes(result *circuit.Circuit, node *circuit.Node) (*nodeMapping, error) {
	op := node.Operator

	switch op.Linearity() {
	case operator.Linear:
		result.AddNode(circuit.Op(node.ID, op))
		return &nodeMapping{outputNode: node.ID, inputNode: node.ID}, nil

	case operator.Bilinear:
		prefix := node.ID

		// Create integrators.
		// The bilinear formula uses ∫a[t-1] and ∫b[t-1], so we need delays after integrators.
		intLeft := prefix + "_int_left"
		intRight := prefix + "_int_right"
		delayLeft := prefix + "_delay_left"
		delayRight := prefix + "_delay_right"
		result.AddNode(circuit.Integrate(intLeft))
		result.AddNode(circuit.Integrate(intRight))
		result.AddNode(circuit.Delay(delayLeft))
		result.AddNode(circuit.Delay(delayRight))

		// Connect integrators to delays.
		result.AddEdge(circuit.NewEdge(intLeft, delayLeft, 0))
		result.AddEdge(circuit.NewEdge(intRight, delayRight, 0))

		// Create three terms.
		// term1: Δa ⊗ ∫b[t-1] (delta_a × delayed_int_b)
		// term2: ∫a[t-1] ⊗ Δb (delayed_int_a × delta_b)
		// term3: Δa ⊗ Δb (delta_a × delta_b)
		term1 := prefix + "_t1"
		term2 := prefix + "_t2"
		term3 := prefix + "_t3"
		result.AddNode(circuit.Op(term1, op))
		result.AddNode(circuit.Op(term2, op))
		result.AddNode(circuit.Op(term3, op))

		// Create sums.
		sum12 := prefix + "_sum12"
		sumAll := prefix + "_sum"
		result.AddNode(circuit.Op(sum12, operator.NewPlus()))
		result.AddNode(circuit.Op(sumAll, operator.NewPlus()))

		// Internal edges between new nodes.
		// term1 + term2.
		result.AddEdge(circuit.NewEdge(term1, sum12, 0))
		result.AddEdge(circuit.NewEdge(term2, sum12, 1))
		// (term1 + term2) + term3.
		result.AddEdge(circuit.NewEdge(sum12, sumAll, 0))
		result.AddEdge(circuit.NewEdge(term3, sumAll, 1))

		return &nodeMapping{outputNode: sumAll, inputNode: ""}, nil

	case operator.NonLinear:
		prefix := node.ID

		// Create nodes: integrate -> op -> differentiate.
		intNode := prefix + "_int"
		opNode := prefix + "_op"
		diffNode := prefix + "_diff"

		result.AddNode(circuit.Integrate(intNode))
		result.AddNode(circuit.Op(opNode, op))
		result.AddNode(circuit.Differentiate(diffNode))

		// Internal edges.
		result.AddEdge(circuit.NewEdge(intNode, opNode, 0))
		result.AddEdge(circuit.NewEdge(opNode, diffNode, 0))

		return &nodeMapping{outputNode: diffNode, inputNode: intNode}, nil
	}

	return nil, fmt.Errorf("unknown linearity for operator %s", node.ID)
}

// createOperatorEdges creates the external edges for an operator.
func createOperatorEdges(c, result *circuit.Circuit, node *circuit.Node, e *circuit.Edge, mapping map[string]*nodeMapping) {
	op := node.Operator
	prefix := node.ID

	switch op.Linearity() {
	case operator.Linear:
		// Simple: use the operator node directly.
		if e.From == node.ID {
			// Outgoing edge.
			toMapping := mapping[e.To]
			result.AddEdge(circuit.NewEdge(node.ID, toMapping.inputNode, e.Port))
		} else if e.To == node.ID {
			// Incoming edge.
			fromMapping := mapping[e.From]
			result.AddEdge(circuit.NewEdge(fromMapping.outputNode, node.ID, e.Port))
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
			intNode := prefix + "_int"
			fromMapping := mapping[e.From]
			result.AddEdge(circuit.NewEdge(fromMapping.outputNode, intNode, e.Port))
		} else if e.From == node.ID {
			// Outgoing edge.
			diffNode := prefix + "_diff"
			toMapping := mapping[e.To]
			result.AddEdge(circuit.NewEdge(diffNode, toMapping.inputNode, e.Port))
		}
	}
}

