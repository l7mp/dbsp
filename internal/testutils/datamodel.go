package testutils

import (
	"fmt"

	"github.com/l7mp/dbsp/datamodel"
)

// StringElem is a simple string-based document for testing.
type StringElem string

func (s StringElem) Hash() string                { return string(s) }
func (s StringElem) PrimaryKey() (string, error) { return string(s), nil }
func (s StringElem) String() string              { return string(s) }
func (s StringElem) Concat(other datamodel.Document) datamodel.Document {
	if o, ok := other.(StringElem); ok {
		return StringElem(string(s) + "," + string(o))
	}
	return s
}
func (s StringElem) Copy() datamodel.Document       { return s }
func (s StringElem) GetField(_ string) (any, error) { return string(s), nil }
func (s StringElem) SetField(_ string, _ any) error { return nil }

// Record is a test document with ID and Value fields.
type Record struct {
	ID    string
	Value int
}

func (r Record) Hash() string                { return r.String() }
func (r Record) PrimaryKey() (string, error) { return r.ID, nil }
func (r Record) String() string              { return fmt.Sprintf("{ID:%s, Value:%d}", r.ID, r.Value) }
func (r Record) Concat(other datamodel.Document) datamodel.Document {
	return Record{ID: r.ID + "," + other.Hash(), Value: r.Value}
}
func (r Record) Copy() datamodel.Document { return r }

func (r Record) GetField(key string) (any, error) {
	switch key {
	case "id", "ID":
		return r.ID, nil
	case "value", "Value":
		return r.Value, nil
	default:
		return nil, datamodel.ErrFieldNotFound
	}
}

func (r Record) SetField(key string, value any) error {
	// Record is immutable (value receiver), so SetField is a no-op.
	// For mutable records, use *Record or a different type.
	return nil
}

// ArrayRecord is a test document with an array field for Unwind testing.
type ArrayRecord struct {
	ID     string
	Values []any
}

func (r ArrayRecord) Hash() string                { return r.String() }
func (r ArrayRecord) PrimaryKey() (string, error) { return r.ID, nil }
func (r ArrayRecord) String() string              { return fmt.Sprintf("{ID:%s, Values:%v}", r.ID, r.Values) }
func (r ArrayRecord) Concat(other datamodel.Document) datamodel.Document {
	return ArrayRecord{ID: r.ID + "," + other.Hash(), Values: r.Values}
}

func (r ArrayRecord) Copy() datamodel.Document {
	values := make([]any, len(r.Values))
	copy(values, r.Values)
	return ArrayRecord{ID: r.ID, Values: values}
}

func (r ArrayRecord) GetField(key string) (any, error) {
	switch key {
	case "id", "ID":
		return r.ID, nil
	case "values", "Values":
		return r.Values, nil
	default:
		return nil, datamodel.ErrFieldNotFound
	}
}

func (r ArrayRecord) SetField(key string, value any) error {
	// ArrayRecord is immutable (value receiver), so SetField is a no-op.
	return nil
}

// MutableRecord is a mutable test document with pointer receiver.
type MutableRecord struct {
	Fields map[string]any
}

func NewMutableRecord(fields map[string]any) *MutableRecord {
	return &MutableRecord{Fields: fields}
}

func (r *MutableRecord) Hash() string { return r.String() }
func (r *MutableRecord) PrimaryKey() (string, error) {
	return r.Hash(), nil
}

func (r *MutableRecord) String() string {
	return fmt.Sprintf("%v", r.Fields)
}

func (r *MutableRecord) Concat(other datamodel.Document) datamodel.Document {
	newFields := make(map[string]any)
	for k, v := range r.Fields {
		newFields[k] = v
	}
	if or, ok := other.(*MutableRecord); ok {
		for k, v := range or.Fields {
			newFields[k] = v
		}
	}
	return &MutableRecord{Fields: newFields}
}

func (r *MutableRecord) Copy() datamodel.Document {
	newFields := make(map[string]any, len(r.Fields))
	for k, v := range r.Fields {
		newFields[k] = v
	}
	return &MutableRecord{Fields: newFields}
}

func (r *MutableRecord) GetField(key string) (any, error) {
	v, ok := r.Fields[key]
	if !ok {
		return nil, datamodel.ErrFieldNotFound
	}
	return v, nil
}

func (r *MutableRecord) SetField(key string, value any) error {
	r.Fields[key] = value
	return nil
}
