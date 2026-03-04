package unstructured

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/datamodel"
)

// Unstructured implements datamodel.Document over a map[string]any (JSON object).
// Fields are stored as a plain Go map and serialised as a JSON object.
// The primary key is computed by an optional function; when that function is
// nil it falls back to Hash().
type Unstructured struct {
	fields map[string]any
	pkFunc func(datamodel.Document) (string, error)
}

// Ensure Unstructured implements datamodel.Document.
var _ datamodel.Document = (*Unstructured)(nil)

// New creates a new Unstructured document. pkFunc may be nil, in which case
// PrimaryKey falls back to Hash(). The provided fields map is deep copied so
// that subsequent mutations of the caller's map do not affect the document.
func New(fields map[string]any, pkFunc func(datamodel.Document) (string, error)) *Unstructured {
	f := make(map[string]any, len(fields))
	for k, v := range fields {
		f[k] = deepCopyAny(v)
	}
	return &Unstructured{fields: f, pkFunc: pkFunc}
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

// Concat merges other's fields into a deep copy of u; right wins on key
// collision. The receiver's pkFunc is preserved in the result.
func (u *Unstructured) Concat(other datamodel.Document) datamodel.Document {
	result := &Unstructured{
		fields: make(map[string]any, len(u.fields)),
		pkFunc: u.pkFunc,
	}
	for k, v := range u.fields {
		result.fields[k] = deepCopyAny(v)
	}
	if ou, ok := other.(*Unstructured); ok {
		for k, v := range ou.fields {
			result.fields[k] = deepCopyAny(v)
		}
	}
	return result
}

// Copy returns a deep copy of the document with the same pkFunc. Nested
// map[string]any and []any values are recursively cloned so that mutations
// of the copy never alias back to the original.
func (u *Unstructured) Copy() datamodel.Document {
	cp := &Unstructured{
		fields: make(map[string]any, len(u.fields)),
		pkFunc: u.pkFunc,
	}
	for k, v := range u.fields {
		cp.fields[k] = deepCopyAny(v)
	}
	return cp
}

// New returns a new empty Unstructured document preserving the pkFunc so that
// documents produced by operators carry the same primary-key logic.
func (u *Unstructured) New() datamodel.Document {
	return &Unstructured{
		fields: make(map[string]any),
		pkFunc: u.pkFunc,
	}
}

// GetField returns the value for a (possibly dotted) field path.
// It returns datamodel.ErrFieldNotFound when the field does not exist.
func (u *Unstructured) GetField(key string) (any, error) {
	parts := strings.SplitN(key, ".", 2)
	v, ok := u.fields[parts[0]]
	if !ok {
		return nil, fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, key)
	}
	if len(parts) == 1 {
		return v, nil
	}
	// Traverse into a nested map.
	nested, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, key)
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
