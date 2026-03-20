package unstructured

// deepCopyAny returns a deep copy of v for all JSON-compatible types.
// Primitive types (string, float64, bool, int64, nil, etc.) are returned
// as-is since they are immutable. map[string]any and []any are recursively
// cloned so that mutations of the copy never affect the original.
func deepCopyAny(v any) any {
	switch val := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(val))
		for k, vv := range val {
			cp[k] = deepCopyAny(vv)
		}
		return cp
	case []any:
		cp := make([]any, len(val))
		for i, vv := range val {
			cp[i] = deepCopyAny(vv)
		}
		return cp
	default:
		// Primitives (string, float64, bool, int64, nil) are immutable; return as-is.
		return v
	}
}
