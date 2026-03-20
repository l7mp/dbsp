package circuit

import "github.com/l7mp/dbsp/engine/operator"

// Node represents a node in the circuit, backed by an Operator.
// The node's role is fully described by its Operator: call node.Operator.Kind()
// to determine the type, and node.Operator.Linearity() to determine how
// incrementalization treats it.
type Node struct {
	ID string
	operator.Operator
}

// Input creates a circuit-input node backed by InputOp.
func Input(id string) *Node {
	return &Node{ID: id, Operator: operator.NewInput()}
}

// Output creates a circuit-output node backed by OutputOp.
func Output(id string) *Node {
	return &Node{ID: id, Operator: operator.NewOutput()}
}

// Op creates a user-defined operator node.
func Op(id string, op operator.Operator) *Node {
	return &Node{ID: id, Operator: op}
}

// Delay creates a delay emit node backed by a placeholder DelayOp.
// When added to a Circuit via AddNode, the circuit replaces the operator
// with a fresh paired (DelayOp, DelayAbsorbOp) sharing internal state,
// and registers the absorb node (id+"_absorb") automatically.
// Edges directed at the emit node ID are transparently rewritten by AddEdge
// to target the absorb node, so callers use a single consistent node ID.
func Delay(id string) *Node {
	emit, _ := operator.NewDelay()
	return &Node{ID: id, Operator: emit}
}

// Integrate creates an integrate (∫) node backed by IntegrateOp.
func Integrate(id string) *Node {
	return &Node{ID: id, Operator: operator.NewIntegrate()}
}

// Differentiate creates a differentiate (D) node backed by DifferentiateOp.
func Differentiate(id string) *Node {
	return &Node{ID: id, Operator: operator.NewDifferentiate()}
}

// Delta0 creates a delta-zero (δ₀) node backed by Delta0Op.
func Delta0(id string) *Node {
	return &Node{ID: id, Operator: operator.NewDelta0()}
}
