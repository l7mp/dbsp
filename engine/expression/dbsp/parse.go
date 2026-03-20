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

// WithRegistry adds a registry to a parser.
func (p *Parser) WithRegistry(r *Registry) *Parser {
	return &Parser{registry: r}
}

// Parse parses JSON into an expression tree.
func (p *Parser) Parse(data []byte) (Expression, error) {
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return p.parseValue(raw)
}

// parseValue converts a raw JSON value to an Expression.
func (p *Parser) parseValue(v any) (Expression, error) {
	switch val := v.(type) {
	case nil:
		return p.callFactory("@nil", nil)

	case bool:
		return p.callFactory("@bool", val)

	case float64:
		// JSON numbers are float64; check if it's actually an int.
		if val == float64(int64(val)) {
			return p.callFactory("@int", int64(val))
		}
		return p.callFactory("@float", val)

	case string:
		if val == "$." {
			return p.callFactory("@copy", nil)
		}
		if val == "$$." {
			return p.callFactory("@subject", nil)
		}
		// Check for $.field shorthand (document field).
		if strings.HasPrefix(val, "$.") {
			return p.callFactory("@get", val[2:])
		}
		// Check for $$.field shorthand (subject field).
		if strings.HasPrefix(val, "$$.") {
			return p.callFactory("@getsub", val[3:])
		}
		return p.callFactory("@string", val)

	case []any:
		// Parse as @list with nested expressions.
		elements := make([]Expression, len(val))
		for i, elem := range val {
			expr, err := p.parseValue(elem)
			if err != nil {
				return nil, fmt.Errorf("list element %d: %w", i, err)
			}
			elements[i] = expr
		}
		return p.callFactory("@list", elements)

	case map[string]any:
		return p.parseMap(val)

	default:
		return nil, fmt.Errorf("unsupported JSON type: %T", v)
	}
}

// parseMap handles JSON objects - either operators or @dict.
func (p *Parser) parseMap(m map[string]any) (Expression, error) {
	// Check for operator: single key starting with @.
	if len(m) == 1 {
		for key, value := range m {
			if strings.HasPrefix(key, "@") {
				return p.parseOperator(key, value)
			}
		}
	}

	// Otherwise, treat as @dict with nested expressions.
	entries := make(map[string]Expression)
	for key, value := range m {
		expr, err := p.parseValue(value)
		if err != nil {
			return nil, fmt.Errorf("dict key %q: %w", key, err)
		}
		entries[key] = expr
	}
	return p.callFactory("@dict", entries)
}

// parseOperator parses an operator expression.
func (p *Parser) parseOperator(name string, rawArgs any) (Expression, error) {
	_, ok := p.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("unknown operator: %s", name)
	}

	args, err := p.prepareArgs(rawArgs)
	if err != nil {
		return nil, fmt.Errorf("operator %s: %w", name, err)
	}

	return p.callFactory(name, args)
}

// prepareArgs converts raw JSON args into the appropriate form for the factory:
// nil, raw value (bool/int64/float64/string), Expression, []Expression, or map[string]Expression.
func (p *Parser) prepareArgs(rawArgs any) (any, error) {
	if rawArgs == nil {
		return nil, nil
	}

	switch args := rawArgs.(type) {
	case []any:
		// List of expressions.
		elements := make([]Expression, len(args))
		for i, elem := range args {
			expr, err := p.parseValue(elem)
			if err != nil {
				return nil, fmt.Errorf("argument %d: %w", i, err)
			}
			elements[i] = expr
		}
		return elements, nil

	case map[string]any:
		// Check if it's an operator (nested expression) or dict args.
		if len(args) == 1 {
			for key := range args {
				if strings.HasPrefix(key, "@") {
					expr, err := p.parseMap(args)
					if err != nil {
						return nil, err
					}
					return expr, nil
				}
			}
		}
		// Treat as dict args.
		entries := make(map[string]Expression)
		for key, value := range args {
			expr, err := p.parseValue(value)
			if err != nil {
				return nil, fmt.Errorf("dict key %q: %w", key, err)
			}
			entries[key] = expr
		}
		return entries, nil

	default:
		// Literal value (bool, float64, string from JSON).
		return args, nil
	}
}

// callFactory calls the factory for the named operator with the given args.
func (p *Parser) callFactory(name string, args any) (Expression, error) {
	factory, ok := p.registry.Get(name)
	if !ok {
		return nil, fmt.Errorf("built-in operator %s not registered", name)
	}
	return factory(args)
}
