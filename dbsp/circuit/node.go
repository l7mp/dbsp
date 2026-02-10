package circuit

import "github.com/l7mp/dbsp/dbsp/operator"

// NodeKind classifies circuit nodes.
type NodeKind int

const (
	// NodeInput represents an input node.
	NodeInput NodeKind = iota
	// NodeOutput represents an output node.
	NodeOutput
	// NodeOperator represents an operator node.
	NodeOperator
	// NodeDelay represents the z^-1 delay operator.
	NodeDelay
	// NodeIntegrate represents the integral operator.
	NodeIntegrate
	// NodeDifferentiate represents the differentiation operator.
	NodeDifferentiate
	// NodeDelta0 represents the delta-zero (initial value injection) operator.
	NodeDelta0
)

// String returns a string representation of the node kind.
func (k NodeKind) String() string {
	switch k {
	case NodeInput:
		return "Input"
	case NodeOutput:
		return "Output"
	case NodeOperator:
		return "Operator"
	case NodeDelay:
		return "Delay"
	case NodeIntegrate:
		return "Integrate"
	case NodeDifferentiate:
		return "Differentiate"
	case NodeDelta0:
		return "Delta0"
	default:
		return "Unknown"
	}
}

// Node represents a node in the circuit.
type Node struct {
	ID       string
	Kind     NodeKind
	Operator operator.Operator // Only for NodeOperator.
}

// Input creates a new input node.
func Input(id string) *Node {
	return &Node{ID: id, Kind: NodeInput}
}

// Output creates a new output node.
func Output(id string) *Node {
	return &Node{ID: id, Kind: NodeOutput}
}

// Op creates a new operator node.
func Op(id string, op operator.Operator) *Node {
	return &Node{ID: id, Kind: NodeOperator, Operator: op}
}

// Delay creates a new delay (z^-1) node.
func Delay(id string) *Node {
	return &Node{ID: id, Kind: NodeDelay}
}

// Integrate creates a new integrate (∫) node.
func Integrate(id string) *Node {
	return &Node{ID: id, Kind: NodeIntegrate}
}

// Differentiate creates a new differentiate (D) node.
func Differentiate(id string) *Node {
	return &Node{ID: id, Kind: NodeDifferentiate}
}

// Delta0 creates a new delta-zero (δ₀) node.
func Delta0(id string) *Node {
	return &Node{ID: id, Kind: NodeDelta0}
}
