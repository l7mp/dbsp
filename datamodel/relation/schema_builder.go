package relation

// NewSchema creates a schema from column names.
func NewSchema(columns ...string) *Schema {
	schema := &Schema{Columns: make([]Column, 0, len(columns))}
	for _, name := range columns {
		schema.Columns = append(schema.Columns, Column{Name: name})
	}
	return schema
}

// WithQualifiedNames fills QualifiedName as table.column when missing.
func (s *Schema) WithQualifiedNames(table string) *Schema {
	if s == nil {
		return s
	}
	for i := range s.Columns {
		if s.Columns[i].QualifiedName == "" {
			s.Columns[i].QualifiedName = table + "." + s.Columns[i].Name
		}
	}
	return s
}

// WithPrimaryKey sets primary key column indices.
func (s *Schema) WithPrimaryKey(indices ...int) *Schema {
	if s == nil {
		return s
	}
	s.PKIndices = append([]int(nil), indices...)
	return s
}
