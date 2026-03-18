package normalize

import (
	"encoding/base64"

	"github.com/l7mp/dbsp/datamodel"
)

// DecodeSecretData decodes base64 values in $.data for Secret-like documents.
// It is a no-op unless kind == "Secret" and $.data is a map.
// Decode failures are ignored per-key and original values are retained.
func DecodeSecretData(doc datamodel.Document) error {
	if doc == nil {
		return nil
	}

	kind, err := doc.GetField("kind")
	if err != nil {
		return nil
	}
	kindStr, ok := kind.(string)
	if !ok || kindStr != "Secret" {
		return nil
	}

	data, err := doc.GetField("data")
	if err != nil {
		return nil
	}
	dataMap, ok := data.(map[string]any)
	if !ok {
		return nil
	}

	for key, value := range dataMap {
		strValue, ok := value.(string)
		if !ok {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(strValue)
		if err != nil {
			continue
		}
		dataMap[key] = string(decoded)
	}

	return nil
}
