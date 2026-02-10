// Package transform provides circuit transformations for DBSP.

package transform

import (
	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/operator"
)

// RewriteRule is a function that attempts one rewrite pass over a circuit.
// It returns true if any changes were made.
type RewriteRule func(c *circuit.Circuit) bool

// Rewrite applies the given rewrite rules to the circuit until no rule makes
// any further changes (fixed-point iteration). It modifies the circuit in place
// and returns the number of passes performed.
func Rewrite(c *circuit.Circuit, rules ...RewriteRule) int {
	passes := 0
	for {
		changed := false
		for _, rule := range rules {
			if rule(c) {
				changed = true
			}
		}
		passes++
		if !changed {
			break
		}
	}
	return passes
}

// DefaultRules returns the basic post-incrementalization rules (D∘I, I∘D, adjacent distinct).
func DefaultRules() []RewriteRule {
	return []RewriteRule{
		EliminateDI,
		EliminateID,
		ConsolidateDistinct,
	}
}

// PreRules returns rules to apply before Incrementalize.
// These consolidate distinct operators using Propositions 6.1 and 6.2.
func PreRules() []RewriteRule {
	return []RewriteRule{
		DistinctPastLinear,
		DistinctDistribution,
		ConsolidateDistinct,
	}
}

// PostRules returns rules to apply after Incrementalize.
// These simplify the incrementalized circuit using Proposition 5.2 push/pull
// rules and D∘I / I∘D cancellation.
func PostRules() []RewriteRule {
	return []RewriteRule{
		SwapDifferentiateLinear,
		SwapLinearIntegrate,
		EliminateDI,
		EliminateID,
		ConsolidateDistinct,
	}
}

// EliminateDI implements Proposition 5.2 rule D ∘ I = id.
// It finds any Differentiate node whose sole predecessor is an Integrate node
// and bypasses both, connecting the Integrate's input directly to the
// Differentiate's outputs.
func EliminateDI(c *circuit.Circuit) bool {
	changed := false
	for _, node := range c.Nodes() {
		if node.Kind != circuit.NodeDifferentiate {
			continue
		}
		pred := uniquePredecessor(c, node.ID)
		if pred == nil || pred.Kind != circuit.NodeIntegrate {
			continue
		}
		// Found I → D pattern. Bypass D first (downstream), then I (upstream).
		if err := c.BypassNode(node.ID); err != nil {
			continue
		}
		if err := c.BypassNode(pred.ID); err != nil {
			continue
		}
		changed = true
	}
	return changed
}

// EliminateID implements Proposition 5.2 rule I ∘ D = id.
// It finds any Integrate node whose sole predecessor is a Differentiate node
// and bypasses both.
func EliminateID(c *circuit.Circuit) bool {
	changed := false
	for _, node := range c.Nodes() {
		if node.Kind != circuit.NodeIntegrate {
			continue
		}
		pred := uniquePredecessor(c, node.ID)
		if pred == nil || pred.Kind != circuit.NodeDifferentiate {
			continue
		}
		// Found D → I pattern. Bypass I first (downstream), then D (upstream).
		if err := c.BypassNode(node.ID); err != nil {
			continue
		}
		if err := c.BypassNode(pred.ID); err != nil {
			continue
		}
		changed = true
	}
	return changed
}

// ConsolidateDistinct implements the rule distinct ∘ distinct = distinct
// (Proposition 6.2). It finds any Distinct operator whose sole predecessor
// is also a Distinct operator and bypasses the upstream one.
func ConsolidateDistinct(c *circuit.Circuit) bool {
	changed := false
	for _, node := range c.Nodes() {
		if !isDistinct(node) {
			continue
		}
		pred := uniquePredecessor(c, node.ID)
		if pred == nil || !isDistinct(pred) {
			continue
		}
		// Found distinct(distinct(...)). Bypass the upstream one.
		if err := c.BypassNode(pred.ID); err != nil {
			continue
		}
		changed = true
	}
	return changed
}

// DistinctPastLinear implements Proposition 6.1.
// It pushes a Distinct operator downstream past a unary linear operator (σ, π)
// by swapping their contents.
func DistinctPastLinear(c *circuit.Circuit) bool {
	changed := false
	for _, node := range c.Nodes() {
		if !isDistinctCommutable(node) {
			continue
		}
		pred := uniquePredecessor(c, node.ID)
		if pred == nil || !isDistinct(pred) {
			continue
		}
		// Found Distinct → Q_linear. Swap to Q_linear → Distinct.
		swapContents(pred, node)
		changed = true
	}
	return changed
}

// DistinctDistribution implements Proposition 6.2.
// When a Distinct has another Distinct downstream separated by a unary linear
// operator, the upstream Distinct is redundant and can be removed.
func DistinctDistribution(c *circuit.Circuit) bool {
	changed := false
	for _, node := range c.Nodes() {
		if !isDistinct(node) {
			continue
		}
		// Check: is predecessor a distinct-commutable linear op?
		pred := uniquePredecessor(c, node.ID)
		if pred == nil || !isDistinctCommutable(pred) {
			continue
		}
		// Check: is grandparent a Distinct?
		grandparent := uniquePredecessor(c, pred.ID)
		if grandparent == nil || !isDistinct(grandparent) {
			continue
		}
		// Found Distinct → Q_linear → Distinct. Bypass the upstream Distinct.
		if err := c.BypassNode(grandparent.ID); err != nil {
			continue
		}
		changed = true
	}
	return changed
}

// SwapDifferentiateLinear implements Proposition 5.2 push rule: D ∘ Q = Q ∘ D
// for unary linear Q. It pushes a Differentiate node downstream past a unary
// linear operator by swapping their contents.
func SwapDifferentiateLinear(c *circuit.Circuit) bool {
	changed := false
	for _, node := range c.Nodes() {
		if !isUnaryLinear(node) {
			continue
		}
		pred := uniquePredecessor(c, node.ID)
		if pred == nil || pred.Kind != circuit.NodeDifferentiate {
			continue
		}
		// Found D → Q_linear. Swap to Q_linear → D.
		swapContents(pred, node)
		changed = true
	}
	return changed
}

// SwapLinearIntegrate implements Proposition 5.2 pull rule: Q ∘ I = I ∘ Q
// for unary linear Q. It pushes an Integrate node upstream past a unary
// linear operator by swapping their contents.
func SwapLinearIntegrate(c *circuit.Circuit) bool {
	changed := false
	for _, node := range c.Nodes() {
		if node.Kind != circuit.NodeIntegrate {
			continue
		}
		pred := uniquePredecessor(c, node.ID)
		if pred == nil || !isUnaryLinear(pred) {
			continue
		}
		// Found Q_linear → I. Swap to I → Q_linear.
		swapContents(pred, node)
		changed = true
	}
	return changed
}

// uniquePredecessor returns the single unique predecessor of a node, or nil if
// the node has zero or multiple different predecessors. Duplicate edges from the
// same source (as produced by Incrementalize) are tolerated.
func uniquePredecessor(c *circuit.Circuit, nodeID string) *circuit.Node {
	inEdges := c.EdgesTo(nodeID)
	if len(inEdges) == 0 {
		return nil
	}
	predID := inEdges[0].From
	for _, e := range inEdges[1:] {
		if e.From != predID {
			return nil
		}
	}
	return c.Node(predID)
}

// swapContents swaps the Kind and Operator fields of two nodes.
// This effectively reorders adjacent unary nodes without rewiring edges.
func swapContents(a, b *circuit.Node) {
	a.Kind, b.Kind = b.Kind, a.Kind
	a.Operator, b.Operator = b.Operator, a.Operator
}

// isDistinct returns true if the node is a Distinct operator.
func isDistinct(n *circuit.Node) bool {
	if n.Kind != circuit.NodeOperator || n.Operator == nil {
		return false
	}
	_, ok := n.Operator.(*operator.Distinct)
	return ok
}

// isDistinctCommutable returns true if the node is a unary linear operator that
// distinct can be pushed past (Props 6.1/6.2): Select (σ) and Project (π).
// Negate is excluded because it flips weight signs, breaking distinct semantics.
func isDistinctCommutable(n *circuit.Node) bool {
	if n.Kind != circuit.NodeOperator || n.Operator == nil {
		return false
	}
	switch n.Operator.(type) {
	case *operator.Select, *operator.Project:
		return true
	default:
		return false
	}
}

// isUnaryLinear returns true if the node is a unary linear operator.
// D and I commute with all linear operators (Prop 5.2).
func isUnaryLinear(n *circuit.Node) bool {
	if n.Kind != circuit.NodeOperator || n.Operator == nil {
		return false
	}
	return n.Operator.Linearity() == operator.Linear && n.Operator.Arity() == 1
}
