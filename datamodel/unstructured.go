package datamodel

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Unstructured implements Document over a map[string]any (JSON object).
// Fields are stored as a plain Go map and serialised as a JSON object.
// The primary key is computed by an optional function; when that function is
// nil it falls back to Hash().
type Unstructured struct {
	fields map[string]any
	pkFunc func(Document) (string, error)
}

// Ensure Unstructured implements Document.
var _ Document = (*Unstructured)(nil)

// NewUnstructured creates a new Unstructured document. pkFunc may be nil, in
// which case PrimaryKey falls back to Hash().
func NewUnstructured(fields map[string]any, pkFunc func(Document) (string, error)) *Unstructured {
	f := make(map[string]any, len(fields))
	for k, v := range fields {
		f[k] = v
	}
	return &Unstructured{fields: f, pkFunc: pkFunc}
}

// ParseUnstructured unmarshals a JSON object and returns an Unstructured
// document with pkFunc=nil.
func ParseUnstructured(data []byte) (*Unstructured, error) {
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("invalid JSON object: %w", err)
	}
	return &Unstructured{fields: fields}, nil
}

// Hash returns a canonical JSON representation of the document's fields.
// encoding/json marshals map keys in sorted order (since Go 1.12), so this
// is deterministic.
func (u *Unstructured) Hash() string {
	b, err := json.Marshal(u.fields)
	if err != nil {
		return fmt.Sprintf("%v", u.fields)
	}
	return string(b)
}

// PrimaryKey calls pkFunc if it is set; otherwise returns Hash().
func (u *Unstructured) PrimaryKey() (string, error) {
	if u.pkFunc != nil {
		return u.pkFunc(u)
	}
	return u.Hash(), nil
}

// String returns the canonical JSON representation of the document.
func (u *Unstructured) String() string {
	return u.Hash()
}

// Concat merges other's fields into a copy of u; right wins on key collision.
// The receiver's pkFunc is preserved in the result.
func (u *Unstructured) Concat(other Document) Document {
	result := &Unstructured{
		fields: make(map[string]any, len(u.fields)),
		pkFunc: u.pkFunc,
	}
	for k, v := range u.fields {
		result.fields[k] = v
	}
	if ou, ok := other.(*Unstructured); ok {
		for k, v := range ou.fields {
			result.fields[k] = v
		}
	}
	return result
}

// Copy returns a shallow copy of the document with the same pkFunc.
func (u *Unstructured) Copy() Document {
	cp := &Unstructured{
		fields: make(map[string]any, len(u.fields)),
		pkFunc: u.pkFunc,
	}
	for k, v := range u.fields {
		cp.fields[k] = v
	}
	return cp
}

// New returns a new empty Unstructured document preserving the pkFunc so that
// documents produced by operators carry the same primary-key logic.
func (u *Unstructured) New() Document {
	return &Unstructured{
		fields: make(map[string]any),
		pkFunc: u.pkFunc,
	}
}

// GetField returns the value for a (possibly dotted) field path.
// It returns ErrFieldNotFound when the field does not exist.
func (u *Unstructured) GetField(key string) (any, error) {
	parts := strings.SplitN(key, ".", 2)
	v, ok := u.fields[parts[0]]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrFieldNotFound, key)
	}
	if len(parts) == 1 {
		return v, nil
	}
	// Traverse into a nested map.
	nested, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrFieldNotFound, key)
	}
	sub := &Unstructured{fields: nested}
	return sub.GetField(parts[1])
}

// SetField sets the value for a top-level field name.
func (u *Unstructured) SetField(key string, value any) error {
	u.fields[key] = value
	return nil
}

// MarshalJSON serialises the document as a JSON object.
func (u *Unstructured) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.fields)
}

// UnmarshalJSON deserialises a JSON object into the document.
func (u *Unstructured) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &u.fields)
}
