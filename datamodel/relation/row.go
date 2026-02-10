package relation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/orderedcode"

	"github.com/l7mp/dbsp/datamodel"
)

// ==========================================
// Schema, Row, and OrderedCode Integration
// ==========================================

// Row implements Document. It uses orderedcode for keys and a slice for data.
type Row struct {
	Table *Table
	Data  []any
}

// Ensure Row implements Document
var _ datamodel.Document = (*Row)(nil)

func (r *Row) Hash() string {
	// Full content key: encode ALL fields
	values := orderedCodeValues(r.Data)
	s, err := orderedcode.Append(nil, values...)
	if err != nil {
		// Fallback for types orderedcode doesn't like, though it supports most
		return fmt.Sprintf("%v", r.Data)
	}
	return string(s)
}

func (r *Row) PrimaryKey() (string, error) {
	schema := r.Table.Schema
	if len(schema.PKIndices) == 0 {
		return "", errors.New("no primary key defined")
	}
	var pkData []any
	for _, idx := range schema.PKIndices {
		if idx >= len(r.Data) {
			return "", errors.New("pk index out of bounds")
		}
		pkData = append(pkData, r.Data[idx])
	}
	pkData = orderedCodeValues(pkData)
	s, err := orderedcode.Append(nil, pkData...)
	if err != nil {
		return "", err
	}
	return string(s), nil
}

// String returns a string representation of the row.
func (r *Row) String() string {
	return fmt.Sprintf("Row%v", r.Data)
}

func (r *Row) Copy() datamodel.Document {
	data := make([]any, len(r.Data))
	copy(data, r.Data)
	return &Row{Table: r.Table, Data: data}
}

func (r *Row) Concat(other datamodel.Document) datamodel.Document {
	r2, ok := other.(*Row)
	if !ok {
		return r
	}

	// Create a temporary Schema and Table but do not register
	schema := Schema{
		Columns:   make([]Column, len(r.Table.Schema.Columns)+len(r2.Table.Schema.Columns)),
		PKIndices: make([]int, len(r.Table.Schema.PKIndices)+len(r2.Table.Schema.PKIndices)),
		Aliases:   make(map[string]string),
	}
	table := NewTable(fmt.Sprintf("%s-%s", r.Table.Name, r2.Table.Name), &schema)

	// Column names are preserved with qualified names, if available.
	for i, c := range r.Table.Schema.Columns {
		schema.Columns[i] = Column{
			Name:          c.Name,
			QualifiedName: qualifyColumn(r.Table, c),
			Type:          c.Type,
		}
	}
	for i, c := range r2.Table.Schema.Columns {
		schema.Columns[len(r.Table.Schema.Columns)+i] = Column{
			Name:          c.Name,
			QualifiedName: qualifyColumn(r2.Table, c),
			Type:          c.Type,
		}
	}

	// Primary keys are the concatenation of the constituent tables' primary key columns
	for i, idx := range r.Table.Schema.PKIndices {
		schema.PKIndices[i] = idx
	}
	for i, idx := range r2.Table.Schema.PKIndices {
		schema.PKIndices[len(r.Table.Schema.PKIndices)+i] = len(r.Table.Schema.Columns) + idx
	}

	// Aliases are just the concatenated aliases prefixed with the table name
	for alias, column := range r.Table.Schema.Aliases {
		schema.Aliases[fmt.Sprintf("%s.%s", r.Table.Name, alias)] = fmt.Sprintf("%s.%s", r.Table.Name, column)
	}
	for alias, column := range r2.Table.Schema.Aliases {
		schema.Aliases[fmt.Sprintf("%s.%s", r2.Table.Name, alias)] = fmt.Sprintf("%s.%s", r2.Table.Name, column)
	}

	return &Row{Table: table, Data: append(r.Data, r2.Data...)}
}

func (r *Row) GetField(field string) (any, error) {
	schema := r.Table.Schema

	for i, col := range schema.Columns {
		if strings.EqualFold(col.QualifiedName, field) || strings.EqualFold(col.Name, field) {
			return r.Data[i], nil
		}
	}
	if schema != nil && schema.Aliases != nil {
		if alias, ok := schema.Aliases[strings.ToLower(field)]; ok {
			for i, col := range schema.Columns {
				if strings.EqualFold(col.QualifiedName, alias) || strings.EqualFold(col.Name, alias) {
					return r.Data[i], nil
				}
			}
		}
	}
	return nil, fmt.Errorf("%w: column %s not found", datamodel.ErrFieldNotFound, field)
}

func (r *Row) SetField(field string, value any) error {
	schema := r.Table.Schema
	for i, col := range schema.Columns {
		if strings.EqualFold(col.QualifiedName, field) || strings.EqualFold(col.Name, field) {
			r.Data[i] = value
			return nil
		}
	}
	if schema != nil && schema.Aliases != nil {
		if alias, ok := schema.Aliases[strings.ToLower(field)]; ok {
			for i, col := range schema.Columns {
				if strings.EqualFold(col.QualifiedName, alias) || strings.EqualFold(col.Name, alias) {
					r.Data[i] = value
					return nil
				}
			}
		}
	}
	return fmt.Errorf("%w: column %s not found", datamodel.ErrFieldNotFound, field)
}

func (s *Schema) AliasForColumn(name string) (string, bool) {
	if s == nil || s.Aliases == nil {
		return "", false
	}
	alias, ok := s.Aliases[strings.ToLower(name)]
	return alias, ok
}

func qualifyColumn(table *Table, column Column) string {
	if column.QualifiedName != "" {
		return column.QualifiedName
	}
	if table == nil {
		return column.Name
	}
	return fmt.Sprintf("%s.%s", table.Name, column.Name)
}

func orderedCodeValues(values []any) []any {
	if len(values) == 0 {
		return values
	}
	converted := make([]any, len(values))
	for i, v := range values {
		switch val := v.(type) {
		case int:
			converted[i] = int64(val)
		case int8:
			converted[i] = int64(val)
		case int16:
			converted[i] = int64(val)
		case int32:
			converted[i] = int64(val)
		case uint:
			converted[i] = uint64(val)
		case uint8:
			converted[i] = uint64(val)
		case uint16:
			converted[i] = uint64(val)
		case uint32:
			converted[i] = uint64(val)
		case float32:
			converted[i] = float64(val)
		default:
			converted[i] = v
		}
	}
	return converted
}
