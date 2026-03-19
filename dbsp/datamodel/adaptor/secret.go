package adaptor

import (
	"encoding/base64"
	"strings"

	"github.com/l7mp/dbsp/dbsp/datamodel"
)

// SecretDataAdaptor returns an adaptor that decodes/encodes Secret .data.* values.
func SecretDataAdaptor(doc datamodel.Document) *Adaptor {
	decode := func(path string, value any) (any, error) {
		if !strings.HasPrefix(path, "data.") {
			return value, nil
		}
		s, ok := value.(string)
		if !ok {
			return value, nil
		}
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return value, nil
		}
		return string(raw), nil
	}
	encode := func(path string, value any) (any, error) {
		if !strings.HasPrefix(path, "data.") {
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
