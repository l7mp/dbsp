package dbsp

import (
	"errors"
	"fmt"

	"github.com/l7mp/dbsp/datamodel"
)

// GetOp implements @get - retrieves a field from the document.
// Also accessible via $.field shorthand syntax.
type GetOp struct{}

func (o *GetOp) Name() string { return "@get" }

func (o *GetOp) Evaluate(ctx *Context, args Args) (any, error) {
	fieldPath, err := getFieldPath(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("%s: no document in context", o.Name())
	}

	value, err := doc.GetField(fieldPath)
	if err != nil {
		// Return the error (including ErrFieldNotFound).
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "error", err)
		return nil, err
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "result", value)
	return value, nil
}

// SetOp implements @set - sets a field on the document (mutates in-place).
// Arguments: [fieldPath, value]
type SetOp struct{}

func (o *SetOp) Name() string { return "@set" }

func (o *SetOp) Evaluate(ctx *Context, args Args) (any, error) {
	elements, err := getBinaryElements(args, o.Name())
	if err != nil {
		return nil, err
	}

	// Evaluate field path.
	fieldPathVal, err := elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: field path: %w", o.Name(), err)
	}
	fieldPath, err := AsString(fieldPathVal)
	if err != nil {
		return nil, fmt.Errorf("%s: field path must be string: %w", o.Name(), err)
	}

	// Evaluate value.
	value, err := elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: value: %w", o.Name(), err)
	}

	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("%s: no document in context", o.Name())
	}

	if err := doc.SetField(fieldPath, value); err != nil {
		return nil, fmt.Errorf("%s: %w", o.Name(), err)
	}

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "value", value)
	return value, nil
}

// GetSubOp implements @getsub - retrieves a field from the subject.
// Also accessible via $$.field shorthand syntax.
type GetSubOp struct{}

func (o *GetSubOp) Name() string { return "@getsub" }

func (o *GetSubOp) Evaluate(ctx *Context, args Args) (any, error) {
	fieldPath, err := getFieldPath(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	subject := ctx.Subject()
	if subject == nil {
		return nil, fmt.Errorf("%s: no subject in context", o.Name())
	}

	// Try to get field from subject.
	// If subject is a Document, use GetField.
	if doc, ok := subject.(datamodel.Document); ok {
		value, err := doc.GetField(fieldPath)
		if err != nil {
			ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "error", err)
			return nil, err
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "result", value)
		return value, nil
	}

	// If subject is a map, get the value.
	if m, ok := subject.(map[string]any); ok {
		value, exists := m[fieldPath]
		if !exists {
			ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "error", "field not found")
			return nil, datamodel.ErrFieldNotFound
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "result", value)
		return value, nil
	}

	return nil, fmt.Errorf("%s: subject is not a document or map: %T", o.Name(), subject)
}

// SetSubOp implements @setsub - sets a field on the subject (mutates in-place).
// Arguments: [fieldPath, value]
type SetSubOp struct{}

func (o *SetSubOp) Name() string { return "@setsub" }

func (o *SetSubOp) Evaluate(ctx *Context, args Args) (any, error) {
	elements, err := getBinaryElements(args, o.Name())
	if err != nil {
		return nil, err
	}

	// Evaluate field path.
	fieldPathVal, err := elements[0].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: field path: %w", o.Name(), err)
	}
	fieldPath, err := AsString(fieldPathVal)
	if err != nil {
		return nil, fmt.Errorf("%s: field path must be string: %w", o.Name(), err)
	}

	// Evaluate value.
	value, err := elements[1].Eval(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: value: %w", o.Name(), err)
	}

	subject := ctx.Subject()
	if subject == nil {
		return nil, fmt.Errorf("%s: no subject in context", o.Name())
	}

	// Try to set field on subject.
	if doc, ok := subject.(datamodel.Document); ok {
		if err := doc.SetField(fieldPath, value); err != nil {
			return nil, fmt.Errorf("%s: %w", o.Name(), err)
		}
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "value", value)
		return value, nil
	}

	// If subject is a map, set the value.
	if m, ok := subject.(map[string]any); ok {
		m[fieldPath] = value
		ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "value", value)
		return value, nil
	}

	return nil, fmt.Errorf("%s: subject is not a document or map: %T", o.Name(), subject)
}

// ExistsOp implements @exists - checks if a field exists in the document.
type ExistsOp struct{}

func (o *ExistsOp) Name() string { return "@exists" }

func (o *ExistsOp) Evaluate(ctx *Context, args Args) (any, error) {
	fieldPath, err := getFieldPath(ctx, args, o.Name())
	if err != nil {
		return nil, err
	}

	doc := ctx.Document()
	if doc == nil {
		return nil, fmt.Errorf("%s: no document in context", o.Name())
	}

	_, fieldErr := doc.GetField(fieldPath)
	exists := !errors.Is(fieldErr, datamodel.ErrFieldNotFound)

	ctx.Logger().V(8).Info("eval", "op", o.Name(), "field", fieldPath, "result", exists)
	return exists, nil
}

// getFieldPath extracts the field path from arguments.
func getFieldPath(ctx *Context, args Args, opName string) (string, error) {
	var value any

	switch a := args.(type) {
	case LiteralArgs:
		value = a.Value
	case UnaryArgs:
		v, err := a.Operand.Eval(ctx)
		if err != nil {
			return "", err
		}
		value = v
	case ListArgs:
		if len(a.Elements) != 1 {
			return "", fmt.Errorf("%s: expected 1 argument, got %d", opName, len(a.Elements))
		}
		v, err := a.Elements[0].Eval(ctx)
		if err != nil {
			return "", err
		}
		value = v
	default:
		return "", fmt.Errorf("%s: unexpected args type %T", opName, args)
	}

	s, err := AsString(value)
	if err != nil {
		return "", fmt.Errorf("%s: field path must be string: %w", opName, err)
	}
	return s, nil
}

func init() {
	MustRegister("@get", func() Operator { return &GetOp{} })
	MustRegister("@set", func() Operator { return &SetOp{} })
	MustRegister("@getsub", func() Operator { return &GetSubOp{} })
	MustRegister("@setsub", func() Operator { return &SetSubOp{} })
	MustRegister("@exists", func() Operator { return &ExistsOp{} })
}
