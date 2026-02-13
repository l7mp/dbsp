package dbsp

import "github.com/l7mp/dbsp/expression"

// ExpressionCompiler adapts the DBSP expression language to expression.Compiler.
type ExpressionCompiler struct {
	registry *Registry
}

// NewCompiler creates a DBSP expression compiler with the default registry.
func NewCompiler() *ExpressionCompiler {
	return &ExpressionCompiler{registry: DefaultRegistry}
}

// WithRegistry sets a custom operator registry. Returns the receiver for chaining.
func (c *ExpressionCompiler) WithRegistry(r *Registry) *ExpressionCompiler {
	c.registry = r
	return c
}

// Compile implements expression.Compiler.
func (c *ExpressionCompiler) Compile(source []byte) (expression.Expression, error) {
	return CompileWithRegistry(source, c.registry)
}

// CompileString implements expression.Compiler.
func (c *ExpressionCompiler) CompileString(source string) (expression.Expression, error) {
	return c.Compile([]byte(source))
}

var _ expression.Compiler = (*ExpressionCompiler)(nil)
