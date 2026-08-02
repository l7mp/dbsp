package datamodel

import (
	"encoding/json"
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
