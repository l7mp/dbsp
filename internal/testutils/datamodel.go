package testutils

import (
	"encoding/json"
	"fmt"

	"github.com/l7mp/dbsp/datamodel"
)

// StringElem is a simple string-based document for testing.
type StringElem string

func (s StringElem) Hash() string                { return string(s) }
func (s StringElem) PrimaryKey() (string, error) { return string(s), nil }
func (s StringElem) String() string              { return string(s) }
func (s StringElem) Copy() datamodel.Document    { return s }
func (s StringElem) Merge(other datamodel.Document) datamodel.Document {
	if o, ok := other.(StringElem); ok {
		return StringElem(string(s) + "," + string(o))
	}
	return s
}
func (s StringElem) New() datamodel.Document        { return StringElem("") }
func (s StringElem) GetField(_ string) (any, error) { return string(s), nil }
func (s StringElem) SetField(_ string, _ any) error { return nil }
func (s StringElem) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s))
}
func (s StringElem) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	_ = value
	return nil
}

// Record is a test document with ID and Value fields.
type Record struct {
	ID    string
	Value int
}

func (r Record) Hash() string                { return r.String() }
func (r Record) PrimaryKey() (string, error) { return r.ID, nil }
func (r Record) String() string              { return fmt.Sprintf("{ID:%s, Value:%d}", r.ID, r.Value) }
func (r Record) Copy() datamodel.Document    { return r }
func (r Record) Merge(other datamodel.Document) datamodel.Document {
	return Record{ID: r.ID + "," + other.Hash(), Value: r.Value}
}
func (r Record) New() datamodel.Document { return Record{} }

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
func (r Record) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"id": r.ID, "value": r.Value})
}
func (r Record) UnmarshalJSON(data []byte) error {
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	_ = fields
	return nil
}
func (r Record) Fields() []string {
	return []string{"id", "value"}
}

// ArrayRecord is a test document with an array field for Unwind testing.
type ArrayRecord struct {
	ID     string
	Values []any
}

func (r ArrayRecord) Hash() string                { return r.String() }
func (r ArrayRecord) PrimaryKey() (string, error) { return r.ID, nil }
func (r ArrayRecord) String() string              { return fmt.Sprintf("{ID:%s, Values:%v}", r.ID, r.Values) }
func (r ArrayRecord) Copy() datamodel.Document {
	values := make([]any, len(r.Values))
	copy(values, r.Values)
	return ArrayRecord{ID: r.ID, Values: values}
}
func (r ArrayRecord) Merge(other datamodel.Document) datamodel.Document {
	return ArrayRecord{ID: r.ID + "," + other.Hash(), Values: r.Values}
}
func (r ArrayRecord) New() datamodel.Document {
	return ArrayRecord{}
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
func (r ArrayRecord) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"id": r.ID, "values": r.Values})
}
func (r ArrayRecord) UnmarshalJSON(data []byte) error {
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	_ = fields
	return nil
}
func (r ArrayRecord) Fields() []string {
	return []string{"id", "values"}
}

// MutableRecord is a mutable test document with pointer receiver.
type MutableRecord struct {
	FieldMap map[string]any
}

func NewMutableRecord(fields map[string]any) *MutableRecord {
	return &MutableRecord{FieldMap: fields}
}

func (r *MutableRecord) Hash() string { return r.String() }
func (r *MutableRecord) PrimaryKey() (string, error) {
	return r.Hash(), nil
}

func (r *MutableRecord) String() string {
	return fmt.Sprintf("%v", r.FieldMap)
}

func (r *MutableRecord) Copy() datamodel.Document {
	newFields := make(map[string]any, len(r.FieldMap))
	for k, v := range r.FieldMap {
		newFields[k] = v
	}
	return &MutableRecord{FieldMap: newFields}
}
func (r *MutableRecord) Merge(other datamodel.Document) datamodel.Document {
	newFields := make(map[string]any, len(r.FieldMap))
	for k, v := range r.FieldMap {
		newFields[k] = v
	}
	if or, ok := other.(*MutableRecord); ok {
		for k, v := range or.FieldMap {
			newFields[k] = v
		}
	}
	return &MutableRecord{FieldMap: newFields}
}
func (r *MutableRecord) New() datamodel.Document {
	return &MutableRecord{FieldMap: make(map[string]any)}
}

func (r *MutableRecord) GetField(key string) (any, error) {
	v, ok := r.FieldMap[key]
	if !ok {
		return nil, datamodel.ErrFieldNotFound
	}
	return v, nil
}

func (r *MutableRecord) SetField(key string, value any) error {
	r.FieldMap[key] = value
	return nil
}
func (r *MutableRecord) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.FieldMap)
}
func (r *MutableRecord) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("MutableRecord must be non-nil for JSON decoding")
	}
	return json.Unmarshal(data, &r.FieldMap)
}
