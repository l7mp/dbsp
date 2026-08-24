package adaptor

import (
	"encoding/base64"
	"fmt"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/ohler55/ojg/jp"
)

// isSecretDataPath reports whether a $-rooted JSONPath addresses a value
// under .data, in any child-fragment spelling ($.data.K, $["data"]["K"],
// $.data["tls.crt"], ...).
func isSecretDataPath(path string) bool {
	expr, err := jp.ParseString(path)
	if err != nil || len(expr) < 3 {
		return false
	}
	child, ok := expr[1].(jp.Child)
	return ok && string(child) == "data"
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
