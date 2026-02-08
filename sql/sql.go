package sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/btree"
	"github.com/google/orderedcode"
	"github.com/l7mp/dbsp/zset"
)

// ==========================================
// Schema, Row, and OrderedCode Integration
// ==========================================

type ColumnType int

const (
	TypeAny ColumnType = iota
	TypeInt
	TypeString
	TypeFloat
)

type Column struct {
	Name string
	Type ColumnType
}

type Schema struct {
	Columns   []Column
	PKIndices []int // Indices of columns that make up the Primary Key
}

// Row implements Document.
// It uses orderedcode for keys and a slice for data.
type Row struct {
	Schema *Schema
	Data   []any
}

// Ensure Row implements Document
var _ zset.Document = (*Row)(nil)

func (r *Row) Key() string {
	// Full content key: encode ALL fields
	s, err := orderedcode.Append(nil, r.Data...)
	if err != nil {
		// Fallback for types orderedcode doesn't like, though it supports most
		return fmt.Sprintf("%v", r.Data)
	}
	return string(s)
}

func (r *Row) PrimaryKey() (string, error) {
	if len(r.Schema.PKIndices) == 0 {
		return "", errors.New("no primary key defined")
	}
	var pkData []any
	for _, idx := range r.Schema.PKIndices {
		if idx >= len(r.Data) {
			return "", errors.New("pk index out of bounds")
		}
		pkData = append(pkData, r.Data[idx])
	}
	s, err := orderedcode.Append(nil, pkData...)
	if err != nil {
		return "", err
	}
	return string(s), nil
}

func (r *Row) GetField(name string) (any, error) {
	for i, col := range r.Schema.Columns {
		if strings.EqualFold(col.Name, name) {
			return r.Data[i], nil
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

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

func (t *Table) Insert(data []any) error {
	if len(data) != len(t.Schema.Columns) {
		return errors.New("column count mismatch")
	}

	// Create Row to generate PK
	r := &Row{Schema: t.Schema, Data: data}
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
