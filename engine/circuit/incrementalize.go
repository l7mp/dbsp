package circuit

import (
	"strings"

	"github.com/l7mp/dbsp/engine/operator"
)

func incrementalID(id string) string {
	return id + "^Δ"
}

// Incrementalize creates the incremental version of n in result and returns
// the routing node IDs (inputNode, outputNode). Both empty means the node is
// bypassed (∫^Δ = I, D^Δ = I).
func (n *Node) Incrementalize(result *Circuit) (inputNode, outputNode string) {
	id := n.ID
	op := n.Operator

	switch {
	case op.Kind() == operator.KindInput:
		result.AddNode(Input(id))
		return id, id
	case op.Kind() == operator.KindOutput:
		result.AddNode(Output(id))
		return id, id
	case op.Kind() == operator.KindDelay:
		incrID := incrementalID(id)
		result.AddNode(Delay(incrID))
		return incrID, incrID
	case op.Kind() == operator.KindDelayAbsorb:
		emitID := strings.TrimSuffix(id, "_absorb")
		incrEmitID := incrementalID(emitID)
		return incrEmitID, incrEmitID + "_absorb"
	case op.Kind() == operator.KindIntegrate, op.Kind() == operator.KindDifferentiate:
		return "", ""
	case op.Kind() == operator.KindDelta0:
		incrID := incrementalID(id)
		result.AddNode(Delta0(incrID))
		return incrID, incrID
	case op.Linearity() == operator.Linear:
		incrID := incrementalID(id)
		result.AddNode(Op(incrID, op))
		return incrID, incrID
	case op.Linearity() == operator.Bilinear:
		prefix := incrementalID(id)
		intLeft := prefix + "_int_left"
		intRight := prefix + "_int_right"
		delayLeft := prefix + "_delay_left"
		delayRight := prefix + "_delay_right"
		term1 := prefix + "_t1"
		term2 := prefix + "_t2"
		term3 := prefix + "_t3"
		sum12 := prefix + "_sum12"
		sumAll := prefix + "_sum"

		result.AddNode(Integrate(intLeft))
		result.AddNode(Integrate(intRight))
		result.AddNode(Delay(delayLeft))
		result.AddNode(Delay(delayRight))
		result.AddNode(Op(term1, op))
		result.AddNode(Op(term2, op))
		result.AddNode(Op(term3, op))
		result.AddNode(Op(sum12, operator.NewPlus()))
		result.AddNode(Op(sumAll, operator.NewPlus()))

		result.AddEdge(NewEdge(intLeft, delayLeft, 0))
		result.AddEdge(NewEdge(intRight, delayRight, 0))
		result.AddEdge(NewEdge(term1, sum12, 0))
		result.AddEdge(NewEdge(term2, sum12, 1))
		result.AddEdge(NewEdge(sum12, sumAll, 0))
		result.AddEdge(NewEdge(term3, sumAll, 1))

		return "", sumAll
	case op.Kind() == operator.KindGroupBy || op.Kind() == operator.KindDistinctPi:
		incrID := incrementalID(id)
		result.AddNode(Op(incrID, op))
		return incrID, incrID
	case op.Linearity() == operator.NonLinear:
		prefix := incrementalID(id)
		intNode := prefix + "_int"
		opNode := prefix + "_op"
		diffNode := prefix + "_diff"

		result.AddNode(Integrate(intNode))
		result.AddNode(Op(opNode, op))
		result.AddNode(Differentiate(diffNode))
		result.AddEdge(NewEdge(intNode, opNode, 0))
		result.AddEdge(NewEdge(opNode, diffNode, 0))

		return intNode, diffNode
	default:
		return "", ""
	}
}
