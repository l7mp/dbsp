package compiler

import (
	"sort"

	"github.com/l7mp/dbsp/dbsp/circuit"
)

// Query is the result of compiling a query source into a DBSP circuit.
type Query struct {
	// Circuit is the DBSP circuit implementing the query.
	Circuit *circuit.Circuit

	// InputMap maps logical input names (e.g., table names) to circuit input node IDs.
	InputMap map[string]string

	// OutputMap maps logical output names to circuit output node IDs.
	OutputMap map[string]string
}

// IR is a compiler intermediate representation.
// Implementations are compiler-specific but share this marker interface.
type IR interface {
	IRKind() string
}

// InputNames returns the logical input names in sorted order.
func (q *Query) InputNames() []string {
	names := make([]string, 0, len(q.InputMap))
	for name := range q.InputMap {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// OutputNames returns the logical output names in sorted order.
func (q *Query) OutputNames() []string {
	names := make([]string, 0, len(q.OutputMap))
	for name := range q.OutputMap {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Compiler turns query sources into DBSP circuits.
type Compiler interface {
	// Parse parses source into an intermediate representation.
	Parse(source []byte) (IR, error)

	// ParseString is a convenience wrapper for string input.
	ParseString(source string) (IR, error)

	// Compile compiles an already-parsed IR into a Query.
	Compile(ir IR) (*Query, error)
}
