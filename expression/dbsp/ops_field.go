package dbsp

import (
	"errors"
	"fmt"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/expression"
)

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
type setExpr struct {
	field Expression
	value Expression
}

func (e *setExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPathVal, err := e.field.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@set: field path: %w", err)
	}
	fieldPath, err := AsString(fieldPathVal)
	if err != nil {
		return nil, fmt.Errorf("@set: field path must be string: %w", err)
	}

	value, err := e.value.Evaluate(ctx)
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

func (e *setExpr) String() string { return fmt.Sprintf("@set(%v, %v)", e.field, e.value) }

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
type setSubExpr struct {
	field Expression
	value Expression
}

func (e *setSubExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPathVal, err := e.field.Evaluate(ctx)
	if err != nil {
		return nil, fmt.Errorf("@setsub: field path: %w", err)
	}
	fieldPath, err := AsString(fieldPathVal)
	if err != nil {
		return nil, fmt.Errorf("@setsub: field path must be string: %w", err)
	}

	value, err := e.value.Evaluate(ctx)
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

func (e *setSubExpr) String() string {
	return fmt.Sprintf("@setsub(%v, %v)", e.field, e.value)
}

// existsExpr implements @exists - checks if a field exists in the document.
type existsExpr struct {
	field Expression
}

func (e *existsExpr) Evaluate(ctx *expression.EvalContext) (any, error) {
	fieldPath, err := evaluateFieldPath(ctx, e.field, "@exists")
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

func (e *existsExpr) String() string { return fmt.Sprintf("@exists(%v)", e.field) }

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
		return &setExpr{field: left, value: right}, nil
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
		return &setSubExpr{field: left, value: right}, nil
	})
	MustRegister("@exists", func(args any) (Expression, error) {
		operand, err := asUnaryExprOrLiteral(args)
		if err != nil {
			return nil, fmt.Errorf("@exists: %w", err)
		}
		return &existsExpr{field: operand}, nil
	})
}
