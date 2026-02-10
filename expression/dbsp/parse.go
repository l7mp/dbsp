package dbsp

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Parser converts JSON to expression trees.
type Parser struct {
	registry *Registry
}

// NewParser creates a parser with the default registry.
func NewParser() *Parser {
	return &Parser{registry: DefaultRegistry}
}

// NewParserWithRegistry creates a parser with a custom registry.
func NewParserWithRegistry(r *Registry) *Parser {
	return &Parser{registry: r}
}

// Parse parses JSON into an expression tree using the default registry.
func Parse(data []byte) (Expr, error) {
	return NewParser().Parse(data)
}

// Parse parses JSON into an expression tree.
func (p *Parser) Parse(data []byte) (Expr, error) {
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return p.parseValue(raw)
}

// parseValue converts a raw JSON value to an Expr.
func (p *Parser) parseValue(v any) (Expr, error) {
	switch val := v.(type) {
	case nil:
		return p.makeLiteralExpr("@nil", nil)

	case bool:
		return p.makeLiteralExpr("@bool", val)

	case float64:
		// JSON numbers are float64; check if it's actually an int.
		if val == float64(int64(val)) {
			return p.makeLiteralExpr("@int", int64(val))
		}
		return p.makeLiteralExpr("@float", val)

	case string:
		// Check for $.field shorthand (document field).
		if strings.HasPrefix(val, "$.") {
			return p.makeOpExpr("@get", LiteralArgs{Value: val[2:]})
		}
		// Check for $$.field shorthand (subject field).
		if strings.HasPrefix(val, "$$.") {
			return p.makeOpExpr("@getsub", LiteralArgs{Value: val[3:]})
		}
		return p.makeLiteralExpr("@string", val)

	case []any:
		// Parse as @list with nested expressions.
		elements := make([]Expr, len(val))
		for i, elem := range val {
			expr, err := p.parseValue(elem)
			if err != nil {
				return nil, fmt.Errorf("list element %d: %w", i, err)
			}
			elements[i] = expr
		}
		return p.makeOpExpr("@list", ListArgs{Elements: elements})

	case map[string]any:
		return p.parseMap(val)

	default:
		return nil, fmt.Errorf("unsupported JSON type: %T", v)
	}
}

// parseMap handles JSON objects - either operators or @dict.
func (p *Parser) parseMap(m map[string]any) (Expr, error) {
	// Check for operator: single key starting with @.
	if len(m) == 1 {
		for key, value := range m {
			if strings.HasPrefix(key, "@") {
				return p.parseOperator(key, value)
			}
		}
	}

	// Otherwise, treat as @dict with nested expressions.
	entries := make(map[string]Expr)
	for key, value := range m {
		expr, err := p.parseValue(value)
		if err != nil {
			return nil, fmt.Errorf("dict key %q: %w", key, err)
		}
		entries[key] = expr
	}
	return p.makeOpExpr("@dict", DictArgs{Entries: entries})
}

// parseOperator parses an operator expression.
func (p *Parser) parseOperator(name string, rawArgs any) (Expr, error) {
	factory, ok := p.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("unknown operator: %s", name)
	}

	op := factory()
	args, err := p.parseArgs(rawArgs)
	if err != nil {
		return nil, fmt.Errorf("operator %s: %w", name, err)
	}

	return &opExpr{op: op, args: args}, nil
}

// parseArgs parses operator arguments into the appropriate Args type.
func (p *Parser) parseArgs(rawArgs any) (Args, error) {
	if rawArgs == nil {
		return LiteralArgs{Value: nil}, nil
	}

	switch args := rawArgs.(type) {
	case []any:
		// List of expressions.
		elements := make([]Expr, len(args))
		for i, elem := range args {
			expr, err := p.parseValue(elem)
			if err != nil {
				return nil, fmt.Errorf("argument %d: %w", i, err)
			}
			elements[i] = expr
		}
		return ListArgs{Elements: elements}, nil

	case map[string]any:
		// Check if it's an operator (nested expression) or dict args.
		if len(args) == 1 {
			for key := range args {
				if strings.HasPrefix(key, "@") {
					expr, err := p.parseMap(args)
					if err != nil {
						return nil, err
					}
					return UnaryArgs{Operand: expr}, nil
				}
			}
		}
		// Treat as dict args.
		entries := make(map[string]Expr)
		for key, value := range args {
			expr, err := p.parseValue(value)
			if err != nil {
				return nil, fmt.Errorf("dict key %q: %w", key, err)
			}
			entries[key] = expr
		}
		return DictArgs{Entries: entries}, nil

	default:
		// Literal value.
		return LiteralArgs{Value: args}, nil
	}
}

// makeLiteralExpr creates a literal expression.
func (p *Parser) makeLiteralExpr(opName string, value any) (Expr, error) {
	factory, ok := p.registry.Get(opName)
	if !ok {
		return nil, fmt.Errorf("built-in operator %s not registered", opName)
	}
	return &literalExpr{op: factory(), value: value}, nil
}

// makeOpExpr creates an operator expression.
func (p *Parser) makeOpExpr(opName string, args Args) (Expr, error) {
	factory, ok := p.registry.Get(opName)
	if !ok {
		return nil, fmt.Errorf("built-in operator %s not registered", opName)
	}
	return &opExpr{op: factory(), args: args}, nil
}
