package datamodel

import (
	"encoding/json"
	"reflect"
)

// DeepCopyAny returns a deep copy of a JSON-shaped value: map[string]any
// and []any are recursively cloned so that mutations of the copy never
// affect the original; primitives (string, float64, bool, int64, nil,
// etc.) are immutable and returned as-is.
func DeepCopyAny(v any) any {
	switch val := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(val))
		for k, vv := range val {
			cp[k] = DeepCopyAny(vv)
		}
		return cp
	case []any:
		cp := make([]any, len(val))
		for i, vv := range val {
			cp[i] = DeepCopyAny(vv)
		}
		return cp
	default:
		return v
	}
}

// DeepEqual reports value equality of two JSON-shaped values in canonical
// JSON semantics, computed structurally instead of through serialization:
// numbers compare by value whatever Go numeric type carries them (the
// int64/float64 split is a decode-path artifact, not data), maps and lists
// compare element-wise through the same normalization, and everything else
// compares strictly. Two values are DeepEqual exactly when their
// NormalizeAny forms are reflect.DeepEqual.
func DeepEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if ai, aInt := asInt(a); aInt {
		if bi, bInt := asInt(b); bInt {
			return ai == bi
		}
	}
	if af, aNum := asFloat(a); aNum {
		bf, bNum := asFloat(b)
		if bNum {
			return af == bf
		}
		return false
	}

	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			bvv, ok := bv[k]
			if !ok || !DeepEqual(v, bvv) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !DeepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	}

	return reflect.DeepEqual(a, b)
}

// asInt returns v as an int64 when it is an integer type (bools and numeric
// strings are not numbers).
func asInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), true
	}
	return 0, false
}

// asFloat returns v as a float64 when it is any numeric type.
func asFloat(v any) (float64, bool) {
	if i, ok := asInt(v); ok {
		return float64(i), true
	}
	switch n := v.(type) {
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// NormalizeAny returns the canonical JSON form of a value: the value
// serialized to JSON and parsed back, so every number lands as float64,
// every nested value is JSON-shaped, and documents materialize their
// content. The result shares no memory with the input. Two values with
// equal canonical JSON normalize to deep-equal results, whatever Go types
// they started from.
func NormalizeAny(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Normalize is NormalizeAny for a document content map. A nil map
// normalizes to an empty one.
func Normalize(m map[string]any) (map[string]any, error) {
	if m == nil {
		return map[string]any{}, nil
	}
	out, err := NormalizeAny(m)
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}
