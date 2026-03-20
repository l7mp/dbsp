package relation

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/btree"
	"github.com/l7mp/dbsp/engine/zset"
)

// ==========================================
// Table & Storage (BTree)
// ==========================================

// Item wrapper for Google BTree
type tableItem struct {
	pk  string
	row *Row
}

func (t tableItem) Less(than btree.Item) bool {
	return t.pk < than.(tableItem).pk
}

type Table struct {
	Name   string
	Schema *Schema
	// The BTree stores `tableItem`
	store *btree.BTree
}

// MarshalJSON implements json.Marshaler.
func (t *Table) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	if t.Schema == nil {
		return nil, fmt.Errorf("table schema required for JSON encoding")
	}

	rows := make([]*Row, 0)
	if t.store != nil {
		t.store.Ascend(func(i btree.Item) bool {
			item := i.(tableItem)
			rows = append(rows, item.row)
			return true
		})
	}

	payload := struct {
		Name   string  `json:"name"`
		Schema *Schema `json:"schema"`
		Rows   []*Row  `json:"rows"`
	}{
		Name:   t.Name,
		Schema: t.Schema,
		Rows:   rows,
	}

	return json.Marshal(payload)
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *Table) UnmarshalJSON(data []byte) error {
	if t == nil {
		return fmt.Errorf("table must be non-nil for JSON decoding")
	}

	var payload struct {
		Name   string            `json:"name"`
		Schema *Schema           `json:"schema"`
		Rows   []json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	if payload.Schema == nil {
		return fmt.Errorf("table schema required")
	}
	if payload.Name == "" {
		return fmt.Errorf("table name required")
	}

	t.Name = payload.Name
	t.Schema = payload.Schema
	t.store = btree.New(2)

	if len(payload.Rows) == 0 {
		return nil
	}

	rowTable := NewTable(t.Name, t.Schema)
	for _, raw := range payload.Rows {
		row := &Row{Table: rowTable}
		if err := json.Unmarshal(raw, row); err != nil {
			return err
		}
		if err := t.InsertRow(row); err != nil {
			return err
		}
	}

	return nil
}

func NewTable(name string, schema *Schema) *Table {
	return &Table{
		Name:   name,
		Schema: schema,
		store:  btree.New(2), // Degree 2 for simple testing
	}
}

// Claim reassigns a row to this table with optional compatibility checks.
func (t *Table) Claim(row *Row) (*Row, error) {
	if row == nil {
		return nil, errors.New("row is nil")
	}
	if len(row.Data) != len(t.Schema.Columns) {
		return nil, errors.New("column count mismatch")
	}
	claimed := row.Copy().(*Row)
	claimed.Table = t
	return claimed, nil
}

func (t *Table) Insert(data []any) error {
	if len(data) != len(t.Schema.Columns) {
		return errors.New("column count mismatch")
	}

	// Create Row to generate PK
	r := &Row{Table: t, Data: data}
	pk, err := r.PrimaryKey()
	if err != nil {
		return err
	}

	t.store.ReplaceOrInsert(tableItem{
		pk:  pk,
		row: r,
	})
	return nil
}

// InsertRow inserts a row into the table after claiming it.
func (t *Table) InsertRow(row *Row) error {
	claimed, err := t.Claim(row)
	if err != nil {
		return err
	}
	pk, err := claimed.PrimaryKey()
	if err != nil {
		return err
	}
	t.store.ReplaceOrInsert(tableItem{pk: pk, row: claimed})
	return nil
}

// Delete removes a row by primary key.
func (t *Table) Delete(pk string) bool {
	item := t.store.Delete(tableItem{pk: pk})
	return item != nil
}

// Update updates a row by primary key using the updater callback.
func (t *Table) Update(pk string, fn func(row *Row) error) (*Row, error) {
	item := t.store.Get(tableItem{pk: pk})
	if item == nil {
		return nil, errors.New("row not found")
	}
	current := item.(tableItem).row
	if err := fn(current); err != nil {
		return nil, err
	}
	newPk, err := current.PrimaryKey()
	if err != nil {
		return nil, err
	}
	if newPk != pk {
		t.store.Delete(tableItem{pk: pk})
		t.store.ReplaceOrInsert(tableItem{pk: newPk, row: current})
	}
	return current, nil
}

// Lookup returns a row by primary key.
func (t *Table) Lookup(pk string) (*Row, bool) {
	item := t.store.Get(tableItem{pk: pk})
	if item == nil {
		return nil, false
	}
	return item.(tableItem).row, true
}

// ToZSet converts the Table to a ZSet with weight 1.
func (t *Table) ToZSet() zset.ZSet {
	z := zset.New()
	t.store.Ascend(func(i btree.Item) bool {
		item := i.(tableItem)
		z.Insert(item.row, 1)
		return true
	})
	return z
}

// ZSetToTable attempts to load a ZSet into a Table.
// Note: This assumes the Documents in the ZSet are compatible with the Table Schema.
func ZSetToTable(z zset.ZSet, t *Table) error {
	// Clear existing? For this toy, let's just insert.
	for _, value := range z.Entries() {
		// If entry.weight <= 0, we skip in a real DB, but here we just insert.
		// We need to extract data. If Document is *Row, easy.
		row, ok := value.Document.(*Row)
		if !ok {
			return errors.New("ZSet contains non-Row documents, cannot hydrate Table")
		}
		// We use the Table's schema, not the Row's (though they should match)
		if err := t.Insert(row.Data); err != nil {
			return err
		}
	}
	return nil
}
