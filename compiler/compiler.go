package compiler

import (
	"sort"

	"github.com/l7mp/dbsp/dbsp/circuit"
)

// CompiledQuery is the result of compiling a query source into a DBSP circuit.
type CompiledQuery struct {
	// Circuit is the DBSP circuit implementing the query.
	Circuit *circuit.Circuit

	// InputMap maps logical input names (e.g., table names) to circuit input node IDs.
	InputMap map[string]string

	// OutputMap maps logical output names to circuit output node IDs.
	OutputMap map[string]string
}

// InputNames returns the logical input names in sorted order.
func (q *CompiledQuery) InputNames() []string {
	names := make([]string, 0, len(q.InputMap))
	for name := range q.InputMap {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// OutputNames returns the logical output names in sorted order.
func (q *CompiledQuery) OutputNames() []string {
	names := make([]string, 0, len(q.OutputMap))
	for name := range q.OutputMap {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Compiler turns query sources into DBSP circuits.
type Compiler interface {
	// Compile parses a query and returns a CompiledQuery.
	Compile(source []byte) (*CompiledQuery, error)

	// CompileString is a convenience wrapper for string input.
	CompileString(source string) (*CompiledQuery, error)
}
