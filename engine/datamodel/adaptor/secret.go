package adaptor

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/engine/datamodel"
)

// isSecretDataPath reports whether a canonical $-rooted JSONPath addresses
// a value under .data, both child forms: $.data.K and $["data"][...].
func isSecretDataPath(path string) bool {
	return strings.HasPrefix(path, "$.data.") || strings.HasPrefix(path, `$["data"]`)
}

// SecretDataAdaptor returns an adaptor that decodes/encodes Secret .data.* values.
func SecretDataAdaptor(doc datamodel.Document) *Adaptor {
	decode := func(path string, value any) (any, error) {
		if !isSecretDataPath(path) {
			return value, nil
		}
		s, ok := value.(string)
		if !ok {
			return value, nil
		}
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return value, fmt.Errorf("decode secret data %q: %w", path, err)
		}
		return string(raw), nil
	}
	encode := func(path string, value any) (any, error) {
		if !isSecretDataPath(path) {
			return value, nil
		}
		s, ok := value.(string)
		if !ok {
			return value, nil
		}
		return base64.StdEncoding.EncodeToString([]byte(s)), nil
	}
	return New(doc, decode, encode)
}
