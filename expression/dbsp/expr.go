package dbsp

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/expression"
)

// Operator is the interface for all expression operators.
type Operator interface {
	// Name returns the operator name (e.g., "@add").
	Name() string

	// Evaluate executes the operator with the given context and arguments.
	// The arguments have already been parsed but NOT evaluated.
	// The operator decides when/how to evaluate its arguments.
	Evaluate(ctx *Context, args Args) (any, error)
}

// Args represents typed arguments to an operator.
type Args interface {
	isArgs()
}

// LiteralArgs holds a literal value (no sub-expressions).
type LiteralArgs struct {
	Value any
}

func (LiteralArgs) isArgs() {}

// UnaryArgs holds a single expression argument.
type UnaryArgs struct {
	Operand Expr
}

func (UnaryArgs) isArgs() {}

// ListArgs holds a list of expression arguments.
type ListArgs struct {
	Elements []Expr
}

func (ListArgs) isArgs() {}

// DictArgs holds key-expression pairs.
type DictArgs struct {
	Entries map[string]Expr
}

func (DictArgs) isArgs() {}

// Expr is an expression node in the AST.
type Expr interface {
	fmt.Stringer

	// Eval evaluates the expression in the given context.
	Eval(ctx *Context) (any, error)

	// Op returns the operator.
	Op() Operator

	// Args returns the arguments.
	Args() Args
}

// NewLiteralExpr creates a literal expression with the given operator.
func NewLiteralExpr(op Operator, value any) Expr {
	return &literalExpr{op: op, value: value}
}

// NewOpExpr creates an expression with the given operator and arguments.
func NewOpExpr(op Operator, args Args) Expr {
	return &opExpr{op: op, args: args}
}

// literalExpr is an expression that holds a literal value.
type literalExpr struct {
	op    Operator
	value any
}

func (e *literalExpr) Eval(ctx *Context) (any, error) {
	return e.op.Evaluate(ctx, LiteralArgs{Value: e.value})
}

func (e *literalExpr) Op() Operator { return e.op }
func (e *literalExpr) Args() Args   { return LiteralArgs{Value: e.value} }
func (e *literalExpr) String() string {
	return fmt.Sprintf("%s(%v)", e.op.Name(), e.value)
}

// opExpr is an expression with an operator and arguments.
type opExpr struct {
	op   Operator
	args Args
}

func (e *opExpr) Eval(ctx *Context) (any, error) {
	return e.op.Evaluate(ctx, e.args)
}

func (e *opExpr) Op() Operator { return e.op }
func (e *opExpr) Args() Args   { return e.args }
func (e *opExpr) String() string {
	return fmt.Sprintf("%s(%v)", e.op.Name(), e.args)
}

// Expression adapts Expr to expression.Expression interface.
// This is the public-facing type that DBSP operators use.
type Expression struct {
	root   Expr
	logger logr.Logger
}

// NewExpression wraps a DBSP expression root with a default logger.
func NewExpression(root Expr) *Expression {
	return &Expression{root: root, logger: logr.Discard()}
}

// Compile parses JSON and returns an Expression using the default registry.
func Compile(data []byte) (*Expression, error) {
	return CompileWithRegistry(data, DefaultRegistry)
}

// CompileString parses a JSON string and returns an Expression using the default registry.
func CompileString(s string) (*Expression, error) {
	return Compile([]byte(s))
}

// CompileWithRegistry parses JSON using a custom registry.
func CompileWithRegistry(data []byte, registry *Registry) (*Expression, error) {
	parser := NewParser().WithRegistry(registry)
	root, err := parser.Parse(data)
	if err != nil {
		return nil, err
	}
	return &Expression{root: root, logger: logr.Discard()}, nil
}

// CompileStringWithRegistry parses a JSON string using a custom registry.
func CompileStringWithRegistry(s string, registry *Registry) (*Expression, error) {
	return CompileWithRegistry([]byte(s), registry)
}

// WithLogger returns a new Expression with the given logger.
func (e *Expression) WithLogger(logger logr.Logger) *Expression {
	return &Expression{
		root:   e.root,
		logger: logger,
	}
}

// Evaluate implements expression.Expression.
func (e *Expression) Evaluate(doc datamodel.Document) (any, error) {
	ctx := NewContext(doc).WithLogger(e.logger)
	return e.root.Eval(ctx)
}

// String implements fmt.Stringer.
func (e *Expression) String() string {
	if e.root == nil {
		return "<nil>"
	}
	return e.root.String()
}

// Root returns the root expression node.
func (e *Expression) Root() Expr {
	return e.root
}

// Ensure Expression implements expression.Expression.
var _ expression.Expression = (*Expression)(nil)
