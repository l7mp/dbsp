package relation

import (
	"fmt"

	"github.com/l7mp/dbsp/datamodel"
)

// ConcatRows combines two rows into a joined row with qualified columns.
func ConcatRows(r, r2 *Row) *Row {
	if r == nil {
		return r2
	}
	if r2 == nil {
		return r
	}

	schema := Schema{
		Columns:   make([]Column, len(r.Table.Schema.Columns)+len(r2.Table.Schema.Columns)),
		PKIndices: make([]int, len(r.Table.Schema.PKIndices)+len(r2.Table.Schema.PKIndices)),
		Aliases:   make(map[string]string),
	}
	table := NewTable(fmt.Sprintf("%s-%s", r.Table.Name, r2.Table.Name), &schema)

	for i, c := range r.Table.Schema.Columns {
		schema.Columns[i] = Column{Name: c.Name, QualifiedName: qualifyColumn(r.Table, c), Type: c.Type}
	}
	for i, c := range r2.Table.Schema.Columns {
		schema.Columns[len(r.Table.Schema.Columns)+i] = Column{Name: c.Name, QualifiedName: qualifyColumn(r2.Table, c), Type: c.Type}
	}

	for i, idx := range r.Table.Schema.PKIndices {
		schema.PKIndices[i] = idx
	}
	for i, idx := range r2.Table.Schema.PKIndices {
		schema.PKIndices[len(r.Table.Schema.PKIndices)+i] = len(r.Table.Schema.Columns) + idx
	}

	for alias, column := range r.Table.Schema.Aliases {
		schema.Aliases[fmt.Sprintf("%s.%s", r.Table.Name, alias)] = fmt.Sprintf("%s.%s", r.Table.Name, column)
	}
	for alias, column := range r2.Table.Schema.Aliases {
		schema.Aliases[fmt.Sprintf("%s.%s", r2.Table.Name, alias)] = fmt.Sprintf("%s.%s", r2.Table.Name, column)
	}

	data := make([]any, 0, len(r.Data)+len(r2.Data))
	data = append(data, r.Data...)
	data = append(data, r2.Data...)
	return &Row{Table: table, Data: data}
}

// Merge combines two rows into a joined row with qualified columns.
func (r *Row) Merge(other datamodel.Document) datamodel.Document {
	r2, ok := other.(*Row)
	if !ok {
		return r.Copy()
	}
	return ConcatRows(r, r2)
}
