package dbsp

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// marshalNullaryOp marshals a nullary operator as {"@op": null}.
func marshalNullaryOp(name string) ([]byte, error) {
	return json.Marshal(map[string]any{name: nil})
}

// marshalUnaryOp marshals a unary operator as {"@op": <operand>}.
func marshalUnaryOp(name string, operand Expression) ([]byte, error) {
	opJSON, err := json.Marshal(operand)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{name: opJSON})
}

// marshalBinaryOp marshals a binary operator as {"@op": [left, right]}.
func marshalBinaryOp(name string, left, right Expression) ([]byte, error) {
	return marshalVariadicOp(name, []Expression{left, right})
}

// marshalVariadicOp marshals a variadic operator as {"@op": [args...]}.
func marshalVariadicOp(name string, args []Expression) ([]byte, error) {
	argsJSON, err := marshalExprSlice(args)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{name: argsJSON})
}

// marshalExprSlice marshals a slice of expressions to a JSON array.
func marshalExprSlice(exprs []Expression) (json.RawMessage, error) {
	items := make([]json.RawMessage, len(exprs))
	for i, e := range exprs {
		b, err := json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		items[i] = b
	}
	return json.Marshal(items)
}

// marshalDictNatural marshals dict entries as a plain JSON object {k: v, ...}.
func marshalDictNatural(entries map[string]Expression) ([]byte, error) {
	entriesJSON := make(map[string]json.RawMessage, len(entries))
	for k, v := range entries {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("dict key %q: %w", k, err)
		}
		entriesJSON[k] = b
	}
	return json.Marshal(entriesJSON)
}

// unmarshalInto compiles JSON into an expression and copies the result into dst
// using reflection. dst must be a pointer to the expected concrete expression type.
// All non-constExpr UnmarshalJSON methods delegate here.
func unmarshalInto(b []byte, dst any) error {
	expr, err := Compile(b)
	if err != nil {
		return err
	}
	dstVal := reflect.ValueOf(dst)
	exprVal := reflect.ValueOf(expr)
	if dstVal.Type() != exprVal.Type() {
		return fmt.Errorf("unexpected expression type %T, want %T", expr, dst)
	}
	dstVal.Elem().Set(exprVal.Elem())
	return nil
}

// unmarshalBaseInto compiles JSON into an expression and copies the anonymous
// embedded base field matching base's type into *base. This allows promoted
// UnmarshalJSON methods on base types to work correctly when the receiver is
// the embedded *baseType field of an outer expression type.
func unmarshalBaseInto(b []byte, base any) error {
	expr, err := Compile(b)
	if err != nil {
		return err
	}
	baseVal := reflect.ValueOf(base).Elem()
	exprVal := reflect.ValueOf(expr).Elem()
	for i := 0; i < exprVal.NumField(); i++ {
		f := exprVal.Type().Field(i)
		if f.Anonymous && f.Type == baseVal.Type() {
			baseVal.Set(exprVal.Field(i))
			return nil
		}
	}
	return fmt.Errorf("base type %v not embedded in compiled %T", baseVal.Type(), expr)
}

// nullaryOp is the base type for operators with no arguments.
type nullaryOp struct{ name string }

func (e *nullaryOp) MarshalJSON() ([]byte, error) { return marshalNullaryOp(e.name) }
func (e *nullaryOp) UnmarshalJSON(b []byte) error  { return unmarshalBaseInto(b, e) }
func (e *nullaryOp) String() string                { return e.name }

// unaryOp is the base type for operators with a single operand.
type unaryOp struct {
	name    string
	operand Expression
}

func (e *unaryOp) MarshalJSON() ([]byte, error) { return marshalUnaryOp(e.name, e.operand) }
func (e *unaryOp) UnmarshalJSON(b []byte) error  { return unmarshalBaseInto(b, e) }
func (e *unaryOp) String() string                { return fmt.Sprintf("%s(%v)", e.name, e.operand) }

// binaryOp is the base type for operators with exactly two operands.
type binaryOp struct {
	name  string
	left  Expression
	right Expression
}

func (e *binaryOp) MarshalJSON() ([]byte, error) {
	return marshalBinaryOp(e.name, e.left, e.right)
}
func (e *binaryOp) UnmarshalJSON(b []byte) error { return unmarshalBaseInto(b, e) }
func (e *binaryOp) String() string {
	return fmt.Sprintf("%s(%v, %v)", e.name, e.left, e.right)
}

// variadicOp is the base type for operators with a variable number of operands.
type variadicOp struct {
	name string
	args []Expression
}

func (e *variadicOp) MarshalJSON() ([]byte, error) { return marshalVariadicOp(e.name, e.args) }
func (e *variadicOp) UnmarshalJSON(b []byte) error  { return unmarshalBaseInto(b, e) }
func (e *variadicOp) String() string                { return fmt.Sprintf("%s(%v)", e.name, e.args) }

// constExpr: marshals as the raw literal value.
func (e *constExpr) MarshalJSON() ([]byte, error) { return json.Marshal(e.value) }
func (e *constExpr) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	// JSON numbers are float64; convert integer-valued floats to int64.
	if f, ok := v.(float64); ok && f == float64(int64(f)) {
		v = int64(f)
	}
	e.value = v
	return nil
}

// nilExpr: marshals as JSON null.
func (e *nilExpr) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (e *nilExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// boolExpr: marshals as bare true/false when the operand is a constant bool;
// falls back to {"@bool": <operand>} for computed operands.
func (e *boolExpr) MarshalJSON() ([]byte, error) {
	if c, ok := e.operand.(*constExpr); ok {
		if b, ok := c.value.(bool); ok {
			return json.Marshal(b)
		}
	}
	return marshalUnaryOp("@bool", e.operand)
}
func (e *boolExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// intExpr: marshals as a bare integer when the operand is a constant int64 or
// an integer-valued float64 (which can occur when the explicit form {"@int":42}
// is parsed, since JSON numbers are float64); falls back to {"@int": <operand>}
// for computed operands.
func (e *intExpr) MarshalJSON() ([]byte, error) {
	if c, ok := e.operand.(*constExpr); ok {
		switch v := c.value.(type) {
		case int64:
			return json.Marshal(v)
		case float64:
			if v == float64(int64(v)) {
				return json.Marshal(int64(v))
			}
		}
	}
	return marshalUnaryOp("@int", e.operand)
}
func (e *intExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// floatExpr: marshals as a bare float when the operand is a constant float64;
// falls back to {"@float": <operand>} for computed operands.
func (e *floatExpr) MarshalJSON() ([]byte, error) {
	if c, ok := e.operand.(*constExpr); ok {
		if f, ok := c.value.(float64); ok {
			return json.Marshal(f)
		}
	}
	return marshalUnaryOp("@float", e.operand)
}
func (e *floatExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// stringExpr: marshals as a bare string when the operand is a constant string that
// does not start with "$." or "$$." (which would be misread as a @get/@getsub
// shorthand). Uses {"@string": <operand>} otherwise.
func (e *stringExpr) MarshalJSON() ([]byte, error) {
	if c, ok := e.operand.(*constExpr); ok {
		if s, ok := c.value.(string); ok {
			if !strings.HasPrefix(s, "$.") && !strings.HasPrefix(s, "$$.") {
				return json.Marshal(s)
			}
		}
	}
	return marshalUnaryOp("@string", e.operand)
}
func (e *stringExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// listExpr: marshals as a bare JSON array [<elements...>].
func (e *listExpr) MarshalJSON() ([]byte, error) {
	return marshalExprSlice(e.elements)
}
func (e *listExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// dictExpr: marshals as a bare JSON object {k: v, ...}.
func (e *dictExpr) MarshalJSON() ([]byte, error) { return marshalDictNatural(e.entries) }
func (e *dictExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// getExpr: marshals as "$.field" shorthand when the field is a constant string;
// falls back to {"@get": <field>} for computed field names.
func (e *getExpr) MarshalJSON() ([]byte, error) {
	if c, ok := e.field.(*constExpr); ok {
		if s, ok := c.value.(string); ok {
			return json.Marshal("$." + s)
		}
	}
	return marshalUnaryOp("@get", e.field)
}
func (e *getExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// getSubExpr: marshals as "$$.field" shorthand when the field is a constant string;
// falls back to {"@getsub": <field>} for computed field names.
func (e *getSubExpr) MarshalJSON() ([]byte, error) {
	if c, ok := e.field.(*constExpr); ok {
		if s, ok := c.value.(string); ok {
			return json.Marshal("$$." + s)
		}
	}
	return marshalUnaryOp("@getsub", e.field)
}
func (e *getSubExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// condExpr: marshals as {"@cond": [cond, if-true, if-false]}.
func (e *condExpr) MarshalJSON() ([]byte, error) {
	return marshalVariadicOp("@cond", []Expression{e.cond, e.ifTrue, e.ifFalse})
}
func (e *condExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }
