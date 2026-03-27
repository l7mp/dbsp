package executor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/zset"
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

func formatCircuit(c *circuit.Circuit, schedule []string) string {
	return circuit.FormatExecution(c, schedule)
}

// formatCircuitCompact returns a one-line summary for logging.
//
//nolint:unused
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
