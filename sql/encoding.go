package sql

import (
	"encoding/json"
	"io"

	"github.com/google/btree"
)

// ==========================================
// Serialization (JSONL)
// ==========================================

// DumpTable writes rows as JSON lines
func DumpTable(t *Table, w io.Writer) error {
	enc := json.NewEncoder(w)
	var err error
	t.store.Ascend(func(i btree.Item) bool {
		item := i.(tableItem)
		// We dump the raw data array
		if e := enc.Encode(item.row.Data); e != nil {
			err = e
			return false
		}
		return true
	})
	return err
}

// LoadTable reads JSON lines and inserts into table
func LoadTable(t *Table, r io.Reader) error {
	dec := json.NewDecoder(r)
	for dec.More() {
		var data []any
		if err := dec.Decode(&data); err != nil {
			return err
		}
		// Handle JSON numbers coming in as float64
		// If Schema says Int, we might need to cast, but let's trust "any" for now
		if err := t.Insert(data); err != nil {
			return err
		}
	}
	return nil
}
