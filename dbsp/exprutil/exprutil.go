package exprutil

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/l7mp/dbsp/datamodel"
)

// MapDocument is a Document backed by a string->value map.
// It is intended for lightweight projections.
type MapDocument struct {
	fields map[string]any
}

// NewMapDocument creates a MapDocument from the provided fields.
func NewMapDocument(fields map[string]any) *MapDocument {
	return &MapDocument{fields: fields}
}

// Hash returns a deterministic identifier for the document.
func (d *MapDocument) Hash() string {
	return d.formatFields()
}

// PrimaryKey returns an error because MapDocument has no key.
func (d *MapDocument) PrimaryKey() (string, error) {
	return "", errors.New("primary key unavailable")
}

// String returns a string representation of the document.
func (d *MapDocument) String() string {
	return fmt.Sprintf("Map{%s}", d.formatFields())
}

// Concat merges two documents if the other is a MapDocument.
func (d *MapDocument) Concat(other datamodel.Document) datamodel.Document {
	otherMap, ok := other.(*MapDocument)
	if !ok {
		return d
	}
	merged := make(map[string]any, len(d.fields)+len(otherMap.fields))
	for k, v := range d.fields {
		merged[k] = v
	}
	for k, v := range otherMap.fields {
		merged[k] = v
	}
	return &MapDocument{fields: merged}
}

// Copy returns a deep copy of the document.
func (d *MapDocument) Copy() datamodel.Document {
	copyFields := make(map[string]any, len(d.fields))
	for k, v := range d.fields {
		copyFields[k] = v
	}
	return &MapDocument{fields: copyFields}
}

// GetField returns a field value or ErrFieldNotFound.
func (d *MapDocument) GetField(key string) (any, error) {
	if v, ok := d.fields[key]; ok {
		return v, nil
	}
	return nil, datamodel.ErrFieldNotFound
}

// SetField sets a field value.
func (d *MapDocument) SetField(key string, value any) error {
	if d.fields == nil {
		d.fields = make(map[string]any)
	}
	d.fields[key] = value
	return nil
}

// FieldMap returns a copy of the underlying fields.
func (d *MapDocument) FieldMap() map[string]any {
	if d.fields == nil {
		return nil
	}
	copyFields := make(map[string]any, len(d.fields))
	maps.Copy(copyFields, d.fields)
	return copyFields
}

// Fields returns sorted field names.
func (d *MapDocument) Fields() []string {
	if len(d.fields) == 0 {
		return nil
	}
	keys := make([]string, 0, len(d.fields))
	for k := range d.fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (d *MapDocument) formatFields() string {
	if len(d.fields) == 0 {
		return ""
	}
	keys := make([]string, 0, len(d.fields))
	for k := range d.fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, d.fields[k]))
	}
	return strings.Join(parts, ",")
}

var _ datamodel.Document = (*MapDocument)(nil)
