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

// splitPathRoot recognizes a rooted path: a "$" (document) or "$$"
// (subject) root followed by a child selector ("." or "["). It returns the
// root and the "$"-rooted JSONPath addressing the field within that
// context; ok is false for anything else: a string without a root is a
// literal, never a path.
func splitPathRoot(s string) (root, path string, ok bool) {
	rest, rooted := strings.CutPrefix(s, "$")
	if !rooted {
		return "", "", false
	}
	root = "$"
	if r, subject := strings.CutPrefix(rest, "$"); subject {
		root, rest = "$$", r
	}
	if !strings.HasPrefix(rest, ".") && !strings.HasPrefix(rest, "[") {
		return "", "", false
	}
	return root, "$" + rest, true
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
		// Rooted paths compile to @getField carrying the path verbatim:
		// the root discriminates ("$" reads the document, "$$" the
		// subject). Everything else is a string literal.
		if _, _, ok := splitPathRoot(val); ok {
			return p.callFactory("@getField", val)
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

	args, err := p.prepareArgs(name, rawArgs)
	if err != nil {
		return nil, fmt.Errorf("operator %s: %w", name, err)
	}

	return p.callFactory(name, args)
}

// prepareArgs converts raw JSON args into the appropriate form for the factory:
// nil, raw value (bool/int64/float64/string), Expression, []Expression, or map[string]Expression.
func (p *Parser) prepareArgs(opName string, rawArgs any) (any, error) {
	// @literal takes data, not expressions: its argument is passed through
	// verbatim so that "@"-prefixed keys and "$"-prefixed strings inside it
	// survive uninterpreted.
	if opName == "@literal" {
		return rawArgs, nil
	}

	if rawArgs == nil {
		return nil, nil
	}

	switch args := rawArgs.(type) {
	case []any:
		// List of expressions. The @setField TARGET (first element) stays
		// literal: it is a path by the operator's contract, and parsing it
		// as a value would turn the "$"-rooted string into a read.
		elements := make([]Expression, len(args))
		for i, elem := range args {
			if opName == "@setField" && i == 0 {
				if s, ok := elem.(string); ok {
					elements[i] = &constExpr{value: s}
					continue
				}
			}
			expr, err := p.parseValue(elem)
			if err != nil {
				return nil, fmt.Errorf("argument %d: %w", i, err)
			}
			elements[i] = expr
		}
		return elements, nil

	case map[string]any:
		// Check if it's an operator (nested expression) or dict args. The
		// explicit @dict constructor takes literal entry keys: it exists
		// precisely so "@"-prefixed keys need no escape.
		if opName != "@dict" && len(args) == 1 {
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
		// Scalar literal value (bool, float64, string from JSON).
		// For most operators, "$"-rooted scalar strings are path shorthands
		// compiling to @getField. The operators whose argument IS the path
		// (@getField, @exists) keep it literal: pre-resolving it would
		// double-interpret.
		if s, ok := args.(string); ok && strings.HasPrefix(s, "$") &&
			opName != "@getField" && opName != "@exists" {
			expr, err := p.parseValue(s)
			if err != nil {
				return nil, err
			}
			return expr, nil
		}
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
