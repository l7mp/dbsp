package dbsp

import (
	"encoding/json"
	"fmt"
	"reflect"
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

// marshalDictEntries marshals a dict expression as {"@dict": {k: v, ...}}.
func marshalDictEntries(entries map[string]Expression) ([]byte, error) {
	entriesJSON := make(map[string]json.RawMessage, len(entries))
	for k, v := range entries {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("dict key %q: %w", k, err)
		}
		entriesJSON[k] = b
	}
	entriesBytes, err := json.Marshal(entriesJSON)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{"@dict": entriesBytes})
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
func (e *nilExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// boolExpr: marshals as {"@bool": <operand>}.
func (e *boolExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@bool", e.operand) }
func (e *boolExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// intExpr: marshals as {"@int": <operand>}.
func (e *intExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@int", e.operand) }
func (e *intExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// floatExpr: marshals as {"@float": <operand>}.
func (e *floatExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@float", e.operand) }
func (e *floatExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// stringExpr: marshals as {"@string": <operand>}.
func (e *stringExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@string", e.operand) }
func (e *stringExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// listExpr: marshals as {"@list": [<elements...>]}.
func (e *listExpr) MarshalJSON() ([]byte, error) {
	elems, err := marshalExprSlice(e.elements)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{"@list": elems})
}
func (e *listExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// dictExpr: marshals as {"@dict": {k: v, ...}}.
func (e *dictExpr) MarshalJSON() ([]byte, error) { return marshalDictEntries(e.entries) }
func (e *dictExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// addExpr: marshals as {"@add": [<args...>]}.
func (e *addExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@add", e.args) }
func (e *addExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// subExpr: marshals as {"@sub": [left, right]}.
func (e *subExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@sub", e.left, e.right) }
func (e *subExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// mulExpr: marshals as {"@mul": [<args...>]}.
func (e *mulExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@mul", e.args) }
func (e *mulExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// divExpr: marshals as {"@div": [left, right]}.
func (e *divExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@div", e.left, e.right) }
func (e *divExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// modExpr: marshals as {"@mod": [left, right]}.
func (e *modExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@mod", e.left, e.right) }
func (e *modExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// negExpr: marshals as {"@neg": <operand>}.
func (e *negExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@neg", e.operand) }
func (e *negExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// andExpr: marshals as {"@and": [<args...>]}.
func (e *andExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@and", e.args) }
func (e *andExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// orExpr: marshals as {"@or": [<args...>]}.
func (e *orExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@or", e.args) }
func (e *orExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// notExpr: marshals as {"@not": <operand>}.
func (e *notExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@not", e.operand) }
func (e *notExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// eqExpr: marshals as {"@eq": [left, right]}.
func (e *eqExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@eq", e.left, e.right) }
func (e *eqExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// neqExpr: marshals as {"@neq": [left, right]}.
func (e *neqExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@neq", e.left, e.right) }
func (e *neqExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// gtExpr: marshals as {"@gt": [left, right]}.
func (e *gtExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@gt", e.left, e.right) }
func (e *gtExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// gteExpr: marshals as {"@gte": [left, right]}.
func (e *gteExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@gte", e.left, e.right) }
func (e *gteExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// ltExpr: marshals as {"@lt": [left, right]}.
func (e *ltExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@lt", e.left, e.right) }
func (e *ltExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// lteExpr: marshals as {"@lte": [left, right]}.
func (e *lteExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@lte", e.left, e.right) }
func (e *lteExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// getExpr: marshals as {"@get": <field>}.
func (e *getExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@get", e.field) }
func (e *getExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// setExpr: marshals as {"@set": [field, value]}.
func (e *setExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@set", e.field, e.value) }
func (e *setExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// getSubExpr: marshals as {"@getsub": <field>}.
func (e *getSubExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@getsub", e.field) }
func (e *getSubExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// setSubExpr: marshals as {"@setsub": [field, value]}.
func (e *setSubExpr) MarshalJSON() ([]byte, error) {
	return marshalBinaryOp("@setsub", e.field, e.value)
}
func (e *setSubExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// existsExpr: marshals as {"@exists": <field>}.
func (e *existsExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@exists", e.field) }
func (e *existsExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// condExpr: marshals as {"@cond": [cond, if-true, if-false]}.
func (e *condExpr) MarshalJSON() ([]byte, error) {
	return marshalVariadicOp("@cond", []Expression{e.cond, e.ifTrue, e.ifFalse})
}
func (e *condExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// switchExpr: marshals as {"@switch": [<args...>]}.
func (e *switchExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@switch", e.args) }
func (e *switchExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// definedOrExpr: marshals as {"@definedOr": [<args...>]}.
func (e *definedOrExpr) MarshalJSON() ([]byte, error) {
	return marshalVariadicOp("@definedOr", e.args)
}
func (e *definedOrExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// isNullExpr: marshals as {"@isnull": <operand>}.
func (e *isNullExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@isnull", e.operand) }
func (e *isNullExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// sqlBoolExpr: marshals as {"@sqlbool": <operand>}.
func (e *sqlBoolExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@sqlbool", e.operand) }
func (e *sqlBoolExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// regexpExpr: marshals as {"@regexp": [pattern, str]}.
func (e *regexpExpr) MarshalJSON() ([]byte, error) {
	return marshalBinaryOp("@regexp", e.pattern, e.str)
}
func (e *regexpExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// upperExpr: marshals as {"@upper": <operand>}.
func (e *upperExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@upper", e.operand) }
func (e *upperExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// lowerExpr: marshals as {"@lower": <operand>}.
func (e *lowerExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@lower", e.operand) }
func (e *lowerExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// trimExpr: marshals as {"@trim": <operand>}.
func (e *trimExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@trim", e.operand) }
func (e *trimExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// substringExpr: marshals as {"@substring": [<args...>]}.
func (e *substringExpr) MarshalJSON() ([]byte, error) {
	return marshalVariadicOp("@substring", e.args)
}
func (e *substringExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// replaceExpr: marshals as {"@replace": [<args...>]}.
func (e *replaceExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@replace", e.args) }
func (e *replaceExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// splitExpr: marshals as {"@split": [str, sep]}.
func (e *splitExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@split", e.str, e.sep) }
func (e *splitExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// joinExpr: marshals as {"@join": [list, sep]}.
func (e *joinExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@join", e.list, e.sep) }
func (e *joinExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// startsWithExpr: marshals as {"@startswith": [str, prefix]}.
func (e *startsWithExpr) MarshalJSON() ([]byte, error) {
	return marshalBinaryOp("@startswith", e.str, e.prefix)
}
func (e *startsWithExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// endsWithExpr: marshals as {"@endswith": [str, suffix]}.
func (e *endsWithExpr) MarshalJSON() ([]byte, error) {
	return marshalBinaryOp("@endswith", e.str, e.suffix)
}
func (e *endsWithExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// containsExpr: marshals as {"@contains": [str, sub]}.
func (e *containsExpr) MarshalJSON() ([]byte, error) {
	return marshalBinaryOp("@contains", e.str, e.sub)
}
func (e *containsExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// noopExpr: marshals as {"@noop": null}.
func (e *noopExpr) MarshalJSON() ([]byte, error) { return marshalNullaryOp("@noop") }
func (e *noopExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// argExpr: marshals as {"@arg": null}.
func (e *argExpr) MarshalJSON() ([]byte, error) { return marshalNullaryOp("@arg") }
func (e *argExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// hashExpr: marshals as {"@hash": <operand>}.
func (e *hashExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@hash", e.operand) }
func (e *hashExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// rndExpr: marshals as {"@rnd": [min, max]}.
func (e *rndExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@rnd", e.min, e.max) }
func (e *rndExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// concatExpr: marshals as {"@concat": [<args...>]}.
func (e *concatExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@concat", e.args) }
func (e *concatExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// absExpr: marshals as {"@abs": <operand>}.
func (e *absExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@abs", e.operand) }
func (e *absExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// floorExpr: marshals as {"@floor": <operand>}.
func (e *floorExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@floor", e.operand) }
func (e *floorExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// ceilExpr: marshals as {"@ceil": <operand>}.
func (e *ceilExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@ceil", e.operand) }
func (e *ceilExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// isNilExpr: marshals as {"@isnil": <operand>}.
func (e *isNilExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@isnil", e.operand) }
func (e *isNilExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// mapExpr: marshals as {"@map": [mapFn, list]}.
func (e *mapExpr) MarshalJSON() ([]byte, error) { return marshalBinaryOp("@map", e.mapFn, e.list) }
func (e *mapExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// filterExpr: marshals as {"@filter": [predicate, list]}.
func (e *filterExpr) MarshalJSON() ([]byte, error) {
	return marshalBinaryOp("@filter", e.predicate, e.list)
}
func (e *filterExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// sumExpr: marshals as {"@sum": [<args...>]}.
func (e *sumExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@sum", e.args) }
func (e *sumExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// lenExpr: marshals as {"@len": <operand>}.
func (e *lenExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@len", e.operand) }
func (e *lenExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// minExpr: marshals as {"@min": [<args...>]}.
func (e *minExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@min", e.args) }
func (e *minExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// maxExpr: marshals as {"@max": [<args...>]}.
func (e *maxExpr) MarshalJSON() ([]byte, error) { return marshalVariadicOp("@max", e.args) }
func (e *maxExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// inExpr: marshals as {"@in": [element, list]}.
func (e *inExpr) MarshalJSON() ([]byte, error) {
	return marshalBinaryOp("@in", e.element, e.list)
}
func (e *inExpr) UnmarshalJSON(b []byte) error { return unmarshalInto(b, e) }

// rangeExpr: marshals as {"@range": <operand>}.
func (e *rangeExpr) MarshalJSON() ([]byte, error) { return marshalUnaryOp("@range", e.operand) }
func (e *rangeExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }

// nowExpr: marshals as {"@now": null}.
func (e *nowExpr) MarshalJSON() ([]byte, error) { return marshalNullaryOp("@now") }
func (e *nowExpr) UnmarshalJSON(b []byte) error  { return unmarshalInto(b, e) }
