package relation

import (
	"encoding/json"
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

// MarshalJSON implements json.Marshaler.
func (r *Row) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	if r.Table == nil || r.Table.Schema == nil {
		return nil, fmt.Errorf("row schema required for JSON encoding")
	}
	if len(r.Data) != len(r.Table.Schema.Columns) {
		return nil, fmt.Errorf("row column count mismatch")
	}

	fields := make(map[string]any, len(r.Table.Schema.Columns))
	for i, col := range r.Table.Schema.Columns {
		fields[col.Name] = r.Data[i]
	}
	return json.Marshal(fields)
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *Row) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("row must be non-nil for JSON decoding")
	}
	if r.Table == nil || r.Table.Schema == nil {
		return fmt.Errorf("row schema required for JSON decoding")
	}

	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	values := make([]any, len(r.Table.Schema.Columns))
	for i, col := range r.Table.Schema.Columns {
		value, ok := fields[col.Name]
		if !ok && col.QualifiedName != "" {
			value, ok = fields[col.QualifiedName]
		}
		if !ok {
			return fmt.Errorf("missing column %s", col.Name)
		}
		coerced, err := coerceJSONValue(value, col.Type)
		if err != nil {
			return fmt.Errorf("column %s: %w", col.Name, err)
		}
		values[i] = coerced
	}

	r.Data = values
	return nil
}

func (r *Row) Copy() datamodel.Document {
	data := make([]any, len(r.Data))
	copy(data, r.Data)
	return &Row{Table: r.Table, Data: data}
}

func (r *Row) New() datamodel.Document {
	cols := make([]Column, len(r.Table.Schema.Columns))
	copy(cols, r.Table.Schema.Columns)
	pk := append([]int(nil), r.Table.Schema.PKIndices...)
	newSchema := &Schema{Columns: cols, PKIndices: pk}
	newTable := NewTable(r.Table.Name, newSchema)
	return &Row{Table: newTable, Data: make([]any, len(cols))}
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

// Fields returns column names for this row.
func (r *Row) Fields() []string {
	if r.Table == nil || r.Table.Schema == nil {
		return nil
	}
	fields := make([]string, 0, len(r.Table.Schema.Columns))
	for _, col := range r.Table.Schema.Columns {
		if col.QualifiedName != "" {
			fields = append(fields, col.QualifiedName)
			continue
		}
		fields = append(fields, col.Name)
	}
	return fields
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
		converted[i] = normalizeScalarValue(v)
	}
	return converted
}

func normalizeScalarValue(value any) any {
	switch val := value.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case uint:
		return uint64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case float32:
		return float64(val)
	default:
		return value
	}
}

func coerceJSONValue(value any, columnType ColumnType) (any, error) {
	if columnType == TypeAny {
		return normalizeScalarValue(value), nil
	}

	switch columnType {
	case TypeInt:
		switch v := value.(type) {
		case float64:
			if v != float64(int64(v)) {
				return nil, fmt.Errorf("expected integer, got %v", v)
			}
			return int64(v), nil
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case json.Number:
			parsed, err := v.Int64()
			if err != nil {
				return nil, fmt.Errorf("expected integer: %w", err)
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("expected integer, got %T", value)
		}
	case TypeFloat:
		switch v := value.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case json.Number:
			parsed, err := v.Float64()
			if err != nil {
				return nil, fmt.Errorf("expected float: %w", err)
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("expected float, got %T", value)
		}
	case TypeString:
		if s, ok := value.(string); ok {
			return s, nil
		}
		return nil, fmt.Errorf("expected string, got %T", value)
	default:
		return nil, fmt.Errorf("unsupported column type %v", columnType)
	}
}
