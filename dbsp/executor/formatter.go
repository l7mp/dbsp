package executor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/zset"
)

func formatInputs(inputs []zset.ZSet) string {
	parts := make([]string, len(inputs))
	for i, input := range inputs {
		parts[i] = fmt.Sprintf("%d=%s", i, input.String())
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatZSetMap(values map[string]zset.ZSet) string {
	if len(values) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("{")
	for i, key := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(key)
		b.WriteString(": ")
		b.WriteString(values[key].String())
	}
	b.WriteString("}")
	return b.String()
}

func formatState(state *State) string {
	if state == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"State{delays:%s, integrators:%s, differentiators:%s, delta0:%s}",
		formatZSetMap(state.Delays),
		formatZSetMap(state.Integrators),
		formatZSetMap(state.Differentiators),
		formatBoolMap(state.Delta0Fired),
	)
}

func formatBoolMap(values map[string]bool) string {
	if len(values) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("{")
	for i, key := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(key)
		b.WriteString(": ")
		b.WriteString(fmt.Sprintf("%t", values[key]))
	}
	b.WriteString("}")
	return b.String()
}

func formatCircuit(c *circuit.Circuit, schedule []string) string {
	var b strings.Builder
	b.WriteString("Circuit(")
	b.WriteString(c.Name())
	b.WriteString(")\n")

	// Build incoming edges map for each node.
	incoming := make(map[string][]*circuit.Edge)
	for _, edge := range c.Edges() {
		incoming[edge.To] = append(incoming[edge.To], edge)
	}

	// Sort incoming edges by port.
	for _, edges := range incoming {
		sort.Slice(edges, func(i, j int) bool {
			return edges[i].Port < edges[j].Port
		})
	}

	// Format each node in execution order.
	for i, id := range schedule {
		node := c.Node(id)

		// Tree branch characters.
		prefix := "├─"
		if i == len(schedule)-1 {
			prefix = "└─"
		}

		// Node type indicator.
		var nodeType string
		switch node.Kind {
		case circuit.NodeInput:
			nodeType = "📥"
		case circuit.NodeOutput:
			nodeType = "📤"
		case circuit.NodeOperator:
			nodeType = "⚙️"
		case circuit.NodeDelay:
			nodeType = "⏳"
		case circuit.NodeIntegrate:
			nodeType = "∫"
		case circuit.NodeDifferentiate:
			nodeType = "Δ"
		case circuit.NodeDelta0:
			nodeType = "δ₀"
		default:
			nodeType = "?"
		}

		b.WriteString(prefix)
		b.WriteString(" ")
		b.WriteString(nodeType)
		b.WriteString(" ")
		b.WriteString(id)

		// Add operator details.
		if node.Kind == circuit.NodeOperator && node.Operator != nil {
			b.WriteString(" [")
			b.WriteString(node.Operator.String())
			b.WriteString("]")
		}

		// Add incoming edges.
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

// formatCircuitCompact returns a one-line summary for logging.
func formatCircuitCompact(c *circuit.Circuit, schedule []string) string {
	var b strings.Builder
	b.WriteString("Circuit(")
	b.WriteString(c.Name())
	b.WriteString(") nodes=")
	b.WriteString(fmt.Sprintf("%d", len(schedule)))
	b.WriteString(" edges=")
	b.WriteString(fmt.Sprintf("%d", len(c.Edges())))
	return b.String()
}
