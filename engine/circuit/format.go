package circuit

import (
	"fmt"
	"sort"
	"strings"

	"github.com/l7mp/dbsp/engine/operator"
)

// FormatExecution returns a human-readable, execution-ordered circuit view.
func FormatExecution(c *Circuit, schedule []string) string {
	var b strings.Builder
	b.WriteString("Circuit(")
	b.WriteString(c.Name())
	b.WriteString(")\n")

	// Build incoming edges map for each node.
	incoming := make(map[string][]*Edge)
	for _, edge := range c.Edges() {
		incoming[edge.To] = append(incoming[edge.To], edge)
	}

	// Sort incoming edges by port.
	for _, edges := range incoming {
		sort.Slice(edges, func(i, j int) bool {
			return edges[i].Port < edges[j].Port
		})
	}

	for i, id := range schedule {
		node := c.Node(id)

		prefix := "├─"
		if i == len(schedule)-1 {
			prefix = "└─"
		}

		nodeType := nodeTypeIndicator(node)

		b.WriteString(prefix)
		b.WriteString(" ")
		b.WriteString(nodeType)
		b.WriteString(" ")
		b.WriteString(id)

		if node.Operator != nil {
			k := node.Operator.Kind()
			if k != operator.KindInput && k != operator.KindOutput &&
				k != operator.KindDelay && k != operator.KindDelayAbsorb &&
				k != operator.KindIntegrate && k != operator.KindDifferentiate &&
				k != operator.KindDelta0 && k != operator.KindDistinctH {
				b.WriteString(" [")
				b.WriteString(node.Operator.String())
				b.WriteString("]")
			}
		}

		if edges := incoming[id]; len(edges) > 0 {
			b.WriteString(" ← ")
			for j, e := range edges {
				if j > 0 {
					b.WriteString(", ")
				}
				b.WriteString(e.From)
				if e.Port > 0 {
					b.WriteString(fmt.Sprintf("[%d]", e.Port))
				}
			}
		}

		b.WriteString("\n")
	}

	return b.String()
}

func nodeTypeIndicator(node *Node) string {
	if node == nil || node.Operator == nil {
		return "?"
	}

	switch node.Operator.Kind() {
	case operator.KindInput:
		return "Input"
	case operator.KindOutput:
		return "Output"
	case operator.KindDelay:
		return "z⁻¹"
	case operator.KindDelayAbsorb:
		return "z⁻¹(absorb)"
	case operator.KindIntegrate:
		return "∫"
	case operator.KindDifferentiate:
		return "D"
	case operator.KindDelta0:
		return "δ₀"
	case operator.KindDistinctH:
		return "H"
	default:
		return "op"
	}
}
