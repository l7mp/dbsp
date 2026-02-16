package dbsp

import "github.com/l7mp/dbsp/expression"

// Expression is an alias for expression.Expression used throughout this package.
type Expression = expression.Expression

// Compile parses JSON and returns an Expression using the default registry.
func Compile(data []byte) (Expression, error) {
	return CompileWithRegistry(data, DefaultRegistry)
}

// CompileString parses a JSON string and returns an Expression using the default registry.
func CompileString(s string) (Expression, error) {
	return Compile([]byte(s))
}

// CompileWithRegistry parses JSON using a custom registry.
func CompileWithRegistry(data []byte, registry *Registry) (Expression, error) {
	parser := NewParser().WithRegistry(registry)
	return parser.Parse(data)
}

// CompileStringWithRegistry parses a JSON string using a custom registry.
func CompileStringWithRegistry(s string, registry *Registry) (Expression, error) {
	return CompileWithRegistry([]byte(s), registry)
}
