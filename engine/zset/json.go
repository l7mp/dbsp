package zset

import (
	"encoding/json"
	"fmt"

	"github.com/l7mp/dbsp/engine/datamodel"
)

// UnmarshalWithDocument decodes a ZSet using a concrete document implementation.
func (z *ZSet) UnmarshalWithDocument(data []byte, doc datamodel.Document) error {
	if z == nil {
		return fmt.Errorf("zset must be non-nil for JSON decoding")
	}
	if doc == nil {
		return fmt.Errorf("document prototype required")
	}

	var entries []jsonElem
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}

	if z.entries == nil {
		z.entries = make(map[string]*Elem)
	} else {
		for key := range z.entries {
			delete(z.entries, key)
		}
	}

	for _, entry := range entries {
		if entry.Elem == nil {
			return fmt.Errorf("zset entry elem missing")
		}
		if len(entry.Elem) == 0 {
			return fmt.Errorf("zset entry elem empty")
		}
		clone := doc.New()
		if err := json.Unmarshal(entry.Elem, clone); err != nil {
			return err
		}
		z.Insert(clone, entry.Weight)
	}

	return nil
}
