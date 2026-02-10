package dbsp

import (
	"fmt"
	"reflect"
	"strconv"
)

// AsBool coerces a value to bool.
func AsBool(v any) (bool, error) {
	if v == nil {
		return false, nil
	}

	switch val := v.(type) {
	case bool:
		return val, nil
	case int:
		return val != 0, nil
	case int64:
		return val != 0, nil
	case float64:
		return val != 0, nil
	case string:
		if val == "" {
			return false, nil
		}
		b, err := strconv.ParseBool(val)
		if err != nil {
			return false, fmt.Errorf("cannot convert string %q to bool: %w", val, err)
		}
		return b, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

// AsInt coerces a value to int64.
func AsInt(v any) (int64, error) {
	if v == nil {
		return 0, fmt.Errorf("cannot convert nil to int")
	}

	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	case string:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string %q to int: %w", val, err)
		}
		return i, nil
	case bool:
		if val {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}

// AsFloat coerces a value to float64.
func AsFloat(v any) (float64, error) {
	if v == nil {
		return 0, fmt.Errorf("cannot convert nil to float")
	}

	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string %q to float: %w", val, err)
		}
		return f, nil
	case bool:
		if val {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float", v)
	}
}

// AsString coerces a value to string.
func AsString(v any) (string, error) {
	if v == nil {
		return "", nil
	}

	switch val := v.(type) {
	case string:
		return val, nil
	case int:
		return strconv.FormatInt(int64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(val), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// AsList coerces a value to []any.
func AsList(v any) ([]any, error) {
	if v == nil {
		return nil, nil
	}

	switch val := v.(type) {
	case []any:
		return val, nil
	case []int:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = item
		}
		return result, nil
	case []int64:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = item
		}
		return result, nil
	case []float64:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = item
		}
		return result, nil
	case []string:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = item
		}
		return result, nil
	default:
		// Use reflection for other slice types.
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			result := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				result[i] = rv.Index(i).Interface()
			}
			return result, nil
		}
		return nil, fmt.Errorf("cannot convert %T to list", v)
	}
}

// AsMap coerces a value to map[string]any.
func AsMap(v any) (map[string]any, error) {
	if v == nil {
		return nil, nil
	}

	switch val := v.(type) {
	case map[string]any:
		return val, nil
	case map[string]string:
		result := make(map[string]any, len(val))
		for k, item := range val {
			result[k] = item
		}
		return result, nil
	case map[string]int:
		result := make(map[string]any, len(val))
		for k, item := range val {
			result[k] = item
		}
		return result, nil
	case map[string]int64:
		result := make(map[string]any, len(val))
		for k, item := range val {
			result[k] = item
		}
		return result, nil
	case map[string]float64:
		result := make(map[string]any, len(val))
		for k, item := range val {
			result[k] = item
		}
		return result, nil
	default:
		// Use reflection for other map types with string keys.
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
			result := make(map[string]any, rv.Len())
			for _, key := range rv.MapKeys() {
				result[key.String()] = rv.MapIndex(key).Interface()
			}
			return result, nil
		}
		return nil, fmt.Errorf("cannot convert %T to map", v)
	}
}

// IsInt checks if a value is an integer type.
func IsInt(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

// IsFloat checks if a value is a float type.
func IsFloat(v any) bool {
	switch v.(type) {
	case float32, float64:
		return true
	default:
		return false
	}
}

// IsNumeric checks if a value is numeric (int or float).
func IsNumeric(v any) bool {
	return IsInt(v) || IsFloat(v)
}

// NumericType represents the type of a numeric value.
type NumericType int

const (
	NumericTypeInt NumericType = iota
	NumericTypeFloat
	NumericTypeMixed // One int, one float - result should be float.
)

// GetNumericType returns the common numeric type for two values.
// If both are int, returns NumericTypeInt.
// If both are float or mixed, returns NumericTypeFloat.
func GetNumericType(a, b any) (NumericType, error) {
	aIsInt := IsInt(a)
	aIsFloat := IsFloat(a)
	bIsInt := IsInt(b)
	bIsFloat := IsFloat(b)

	if !aIsInt && !aIsFloat {
		return 0, fmt.Errorf("first operand is not numeric: %T", a)
	}
	if !bIsInt && !bIsFloat {
		return 0, fmt.Errorf("second operand is not numeric: %T", b)
	}

	if aIsInt && bIsInt {
		return NumericTypeInt, nil
	}
	return NumericTypeFloat, nil
}
