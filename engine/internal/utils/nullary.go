package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// ValidateNullaryArgs validates arguments of nullary operators.
//
// Accepted forms are JSON null and empty JSON object ({}), in either raw
// (json.RawMessage) or parser-processed forms.
func ValidateNullaryArgs(args any, opName string) error {
	if IsNullaryArgs(args) {
		return nil
	}

	return fmt.Errorf("%s: argument must be null or empty object", opName)
}

// IsNullaryArgs reports whether args match nullary operator argument shape.
func IsNullaryArgs(args any) bool {
	if args == nil {
		return true
	}

	switch v := args.(type) {
	case json.RawMessage:
		return isNullaryRaw(v)
	case []byte:
		return isNullaryRaw(v)
	}

	rv := reflect.ValueOf(args)
	if rv.Kind() != reflect.Map {
		return false
	}

	if rv.Type().Key().Kind() != reflect.String {
		return false
	}

	return rv.Len() == 0
}

func isNullaryRaw(raw []byte) bool {
	if strings.TrimSpace(string(raw)) == "null" {
		return true
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}

	return len(m) == 0
}
