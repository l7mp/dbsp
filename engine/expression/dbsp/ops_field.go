package dbsp

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/internal/utils"
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

// getExpr implements @get - retrieves a field from the document.
type getExpr struct {
	field Expression
}

func (e *getExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPath, err := evaluateFieldPath(ctx, e.field, "@get")
	if err != nil {
		return nil, err
	}

	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("@get: no document in context")
	}

	value, err := doc.GetField(fieldPath)
	if err != nil {
		ctx.Logger().V(8).Info("eval", "op", "@get", "field", fieldPath, "error", err)
		return nil, err
	}

	ctx.Logger().V(8).Info("eval", "op", "@get", "field", fieldPath, "result", value)
	return value, nil
}

func (e *getExpr) String() string { return fmt.Sprintf("@get(%v)", e.field) }

// setExpr implements @set - sets a field on the document (mutates in-place).
type setExpr struct{ binaryOp }

func (e *setExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPathVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@set: field path: %w", err)
	}
	fieldPath, err := AsString(fieldPathVal)
	if err != nil {
		return nil, fmt.Errorf("@set: field path must be string: %w", err)
	}

	value, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@set: value: %w", err)
	}

	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("@set: no document in context")
	}

	if err := doc.SetField(fieldPath, value); err != nil {
		return nil, fmt.Errorf("@set: %w", err)
	}

	ctx.Logger().V(8).Info("eval", "op", "@set", "field", fieldPath, "value", value)
	return doc, nil
}

// getSubExpr implements @getsub - retrieves a field from the subject.
type getSubExpr struct {
	field Expression
}

func (e *getSubExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPath, err := evaluateFieldPath(ctx, e.field, "@getsub")
	if err != nil {
		return nil, err
	}

	subject := ctx.Subject()
	if subject == nil {
		return nil, fmt.Errorf("@getsub: no subject in context")
	}

	if doc, ok := subject.(datamodel.Document); ok {
		value, err := doc.GetField(fieldPath)
		if err != nil {
			ctx.Logger().V(8).Info("eval", "op", "@getsub", "field", fieldPath, "error", err)
			return nil, err
		}
		ctx.Logger().V(8).Info("eval", "op", "@getsub", "field", fieldPath, "result", value)
		return value, nil
	}

	if m, ok := subject.(map[string]any); ok {
		value, exists := m[fieldPath]
		if !exists {
			ctx.Logger().V(8).Info("eval", "op", "@getsub", "field", fieldPath, "error", "field not found")
			return nil, datamodel.ErrFieldNotFound
		}
		ctx.Logger().V(8).Info("eval", "op", "@getsub", "field", fieldPath, "result", value)
		return value, nil
	}

	return nil, fmt.Errorf("@getsub: subject is not a document or map: %T", subject)
}

func (e *getSubExpr) String() string { return fmt.Sprintf("@getsub(%v)", e.field) }

// setSubExpr implements @setsub - sets a field on the subject (mutates in-place).
type setSubExpr struct{ binaryOp }

func (e *setSubExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPathVal, err := e.left.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@setsub: field path: %w", err)
	}
	fieldPath, err := AsString(fieldPathVal)
	if err != nil {
		return nil, fmt.Errorf("@setsub: field path must be string: %w", err)
	}

	value, err := e.right.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@setsub: value: %w", err)
	}

	subject := ctx.Subject()
	if subject == nil {
		return nil, fmt.Errorf("@setsub: no subject in context")
	}

	if doc, ok := subject.(datamodel.Document); ok {
		if err := doc.SetField(fieldPath, value); err != nil {
			return nil, fmt.Errorf("@setsub: %w", err)
		}
		ctx.Logger().V(8).Info("eval", "op", "@setsub", "field", fieldPath, "value", value)
		return doc, nil
	}

	if m, ok := subject.(map[string]any); ok {
		m[fieldPath] = value
		ctx.Logger().V(8).Info("eval", "op", "@setsub", "field", fieldPath, "value", value)
		return m, nil
	}

	return nil, fmt.Errorf("@setsub: subject is not a document or map: %T", subject)
}

// existsExpr implements @exists - checks if a field exists in the document.
type existsExpr struct{ unaryOp }

func (e *existsExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPath, err := evaluateFieldPath(ctx, e.operand, "@exists")
	if err != nil {
		return nil, err
	}

	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("@exists: no document in context")
	}

	_, fieldErr := doc.GetField(fieldPath)
	exists := !errors.Is(fieldErr, datamodel.ErrFieldNotFound)

	ctx.Logger().V(8).Info("eval", "op", "@exists", "field", fieldPath, "result", exists)
	return exists, nil
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
	MustRegister("@get", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@get: %w", err)
		}
		return &getExpr{field: operand}, nil
	})
	MustRegister("@set", func(args any) (Expression, error) {
		left, right, err := asBinaryExprs(args, "@set")
		if err != nil {
			return nil, err
		}
		return &setExpr{binaryOp{"@set", left, right}}, nil
	})
	MustRegister("@getsub", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@getsub: %w", err)
		}
		return &getSubExpr{field: operand}, nil
	})
	MustRegister("@setsub", func(args any) (Expression, error) {
		left, right, err := asBinaryExprs(args, "@setsub")
		if err != nil {
			return nil, err
		}
		return &setSubExpr{binaryOp{"@setsub", left, right}}, nil
	})
	MustRegister("@exists", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@exists: %w", err)
		}
		return &existsExpr{unaryOp{"@exists", operand}}, nil
	})
}
