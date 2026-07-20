package dbsp

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/internal/utils"
	"github.com/ohler55/ojg/jp"
)

// copyExpr implements @copy - returns document fields as map[string]any.
type copyExpr struct{ nullaryOp }

func (e *copyExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("@copy: no document in context")
	}

	data, err := doc.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("@copy: marshal document: %w", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("@copy: unmarshal document: %w", err)
	}

	ctx.Logger().V(8).Info("eval", "op", "@copy", "result", fields)
	return fields, nil
}

// getFieldExpr implements @getField - reads a field through a $-rooted
// JSONPath. The path carries its root: "$"-rooted paths read the current
// document, "$$"-rooted paths read the current subject (the element under
// iteration in @map/@filter/@sortBy). The shorthand forms "$.x" and "$$.x"
// compile to this operator.
type getFieldExpr struct {
	field Expression
}

func (e *getFieldExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPath, err := evaluateFieldPath(ctx, e.field, "@getField")
	if err != nil {
		return nil, err
	}
	value, err := resolveFieldPath(ctx, fieldPath, "@getField")
	if err != nil {
		ctx.Logger().V(8).Info("eval", "op", "@getField", "field", fieldPath, "error", err)
		return nil, err
	}
	ctx.Logger().V(8).Info("eval", "op", "@getField", "field", fieldPath, "result", value)
	return value, nil
}

func (e *getFieldExpr) String() string { return fmt.Sprintf("@getField(%v)", e.field) }

// resolveFieldPath reads a rooted path from its document: "$$"-rooted paths
// re-root at the subject, "$"-rooted paths read the document, anything else
// is an error — a string without a root is a literal, never a path.
func resolveFieldPath(ctx *expression.EvalContext, fieldPath, opName string) (any, error) {
	root, path, ok := splitPathRoot(fieldPath)
	if !ok {
		return nil, fmt.Errorf("%s: path %q must be a JSONPath rooted at $ (document) or $$ (subject)", opName, fieldPath)
	}
	if root == "$$" {
		doc, err := subjectDocument(ctx, opName)
		if err != nil {
			return nil, err
		}
		return doc.GetField(path)
	}
	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("%s: no document in context", opName)
	}
	return doc.GetField(path)
}

// subjectDocument adapts the evaluation subject to the document interface,
// so that subject paths resolve through the same GetField/SetField
// semantics as document paths. Plain maps — e.g. the synthetic {a, b} pair
// a @sortBy comparator is handed — are wrapped without copying, so
// @setField mutations reach the original value.
func subjectDocument(ctx *expression.EvalContext, opName string) (datamodel.Document, error) {
	subject := ctx.Subject()
	if subject == nil {
		return nil, fmt.Errorf("%s: no subject in context", opName)
	}
	switch s := subject.(type) {
	case datamodel.Document:
		return s, nil
	case map[string]any:
		return unstructured.Wrap(s), nil
	default:
		return nil, fmt.Errorf("%s: subject is not a document or map: %T", opName, subject)
	}
}

// setFieldExpr implements @setField - writes a field through a $-rooted
// JSONPath target, root-discriminated like @getField ("$" writes the
// document and yields it, "$$" writes the subject and yields it). The
// target is kept literal by the parser — in value position a "$"-rooted
// string would be a read — so it arrives here as the path itself.
type setFieldExpr struct{ binaryOp }

func (e *setFieldExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPath, err := evaluateFieldPath(ctx, e.left, "@setField")
	if err != nil {
		return nil, err
	}

	value, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@setField: value: %w", err)
	}

	root, path, ok := splitPathRoot(fieldPath)
	if !ok {
		return nil, fmt.Errorf("@setField: path %q must be a JSONPath rooted at $ (document) or $$ (subject)", fieldPath)
	}
	if root == "$$" {
		doc, err := subjectDocument(ctx, "@setField")
		if err != nil {
			return nil, err
		}
		if err := doc.SetField(path, value); err != nil {
			return nil, fmt.Errorf("@setField: %w", err)
		}
		ctx.Logger().V(8).Info("eval", "op", "@setField", "field", fieldPath, "value", value)
		return ctx.Subject(), nil
	}
	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("@setField: no document in context")
	}
	if err := doc.SetField(path, value); err != nil {
		return nil, fmt.Errorf("@setField: %w", err)
	}
	ctx.Logger().V(8).Info("eval", "op", "@setField", "field", fieldPath, "value", value)
	return doc, nil
}

// existsExpr implements @exists - checks if a field exists. Like every path
// argument, the field is a $-rooted JSONPath, root-discriminated exactly
// like @getField.
type existsExpr struct{ unaryOp }

func (e *existsExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPath, err := evaluateFieldPath(ctx, e.operand, "@exists")
	if err != nil {
		return nil, err
	}

	_, fieldErr := resolveFieldPath(ctx, fieldPath, "@exists")
	if fieldErr != nil && !errors.Is(fieldErr, datamodel.ErrFieldNotFound) {
		// An unresolvable path is an authoring error, not evidence of
		// existence: propagate instead of silently answering true.
		return nil, fmt.Errorf("@exists: %w", fieldErr)
	}
	exists := fieldErr == nil

	ctx.Logger().V(8).Info("eval", "op", "@exists", "field", fieldPath, "result", exists)
	return exists, nil
}

// validatePathArg rejects constant path arguments that are not $-rooted at
// compile time: a string without a root is a literal everywhere in the
// language, and silently reading it as a path would blur that rule.
// Computed paths are validated at evaluation time by resolveFieldPath.
func validatePathArg(field Expression, opName string) error {
	c, ok := field.(*constExpr)
	if !ok {
		return nil
	}
	s, ok := c.value.(string)
	if !ok {
		return nil // Non-string paths fail AsString at evaluation time.
	}
	_, path, ok := splitPathRoot(s)
	if !ok {
		return fmt.Errorf("%s: path %q must be a JSONPath rooted at $ (document) or $$ (subject)", opName, s)
	}
	// The root carries the intent; the JSONPath parser owns validity, so a
	// malformed constant path is a compile error, not a runtime surprise.
	if _, err := jp.ParseString(path); err != nil {
		return fmt.Errorf("%s: invalid JSONPath %q: %w", opName, s, err)
	}
	return nil
}

// evaluateFieldPath evaluates a field path from an operand expression.
func evaluateFieldPath(ctx *expression.EvalContext, field Expression, opName string) (string, error) {
	value, err := field.Evaluate(ctx)
	if err != nil {
		return "", err
	}
	s, err := AsString(value)
	if err != nil {
		return "", fmt.Errorf("%s: field path must be string: %w", opName, err)
	}
	return s, nil
}

func init() {
	MustRegister("@copy", func(args any) (Expression, error) {
		if err := utils.ValidateNullaryArgs(args, "@copy"); err != nil {
			return nil, err
		}
		return &copyExpr{nullaryOp{"@copy"}}, nil
	})
	MustRegister("@getField", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@getField: %w", err)
		}
		if err := validatePathArg(operand, "@getField"); err != nil {
			return nil, err
		}
		return &getFieldExpr{field: operand}, nil
	})
	MustRegister("@setField", func(args any) (Expression, error) {
		left, right, err := asBinaryExprs(args, "@setField")
		if err != nil {
			return nil, err
		}
		if err := validatePathArg(left, "@setField"); err != nil {
			return nil, err
		}
		return &setFieldExpr{binaryOp{"@setField", left, right}}, nil
	})
	MustRegister("@exists", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@exists: %w", err)
		}
		if err := validatePathArg(operand, "@exists"); err != nil {
			return nil, err
		}
		return &existsExpr{unaryOp{"@exists", operand}}, nil
	})
}
