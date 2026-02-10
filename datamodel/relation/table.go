package relation

import (
	"errors"

	"github.com/google/btree"
	"github.com/l7mp/dbsp/dbsp/zset"
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
