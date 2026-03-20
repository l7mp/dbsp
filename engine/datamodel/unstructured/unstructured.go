package unstructured

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ohler55/ojg/jp"

	"github.com/l7mp/dbsp/engine/datamodel"
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

// Merge combines two unstructured documents, with right side overwriting key collisions.
func Merge(left, right *Unstructured) *Unstructured {
	if left == nil {
		if right == nil {
			return New(map[string]any{}, nil)
		}
		return right.Copy().(*Unstructured)
	}
	if right == nil {
		return left.Copy().(*Unstructured)
	}
	res := left.Copy().(*Unstructured)
	for k, v := range right.fields {
		res.fields[k] = deepCopyAny(v)
	}
	return res
}

// Merge combines this document with another; right side overwrites on conflicts.
func (u *Unstructured) Merge(other datamodel.Document) datamodel.Document {
	ou, ok := other.(*Unstructured)
	if !ok {
		return u.Copy()
	}
	return Merge(u, ou)
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

// Fields returns a deep copy of the document fields map.
func (u *Unstructured) Fields() map[string]any {
	f := make(map[string]any, len(u.fields))
	for k, v := range u.fields {
		f[k] = deepCopyAny(v)
	}
	return f
}

// GetField returns the value for a (possibly dotted) field path.
// It returns datamodel.ErrFieldNotFound when the field does not exist.
func (u *Unstructured) GetField(key string) (any, error) {
	if strings.HasPrefix(key, "$") {
		expr, err := jp.ParseString(key)
		if err != nil {
			return nil, fmt.Errorf("invalid JSONPath %q: %w", key, err)
		}
		results := expr.Get(u.fields)
		if len(results) == 0 {
			return nil, fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, key)
		}
		if len(results) == 1 {
			return results[0], nil
		}
		return results, nil
	}

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

// SetField sets the value for a top-level field name, dotted path, or JSONPath.
func (u *Unstructured) SetField(key string, value any) error {
	if strings.HasPrefix(key, "$") {
		expr, err := jp.ParseString(key)
		if err != nil {
			return fmt.Errorf("invalid JSONPath %q: %w", key, err)
		}
		if err := expr.Set(u.fields, value); err != nil {
			return fmt.Errorf("set JSONPath %q: %w", key, err)
		}
		return nil
	}

	if strings.Contains(key, ".") {
		parts := strings.SplitN(key, ".", 2)
		next, ok := u.fields[parts[0]].(map[string]any)
		if !ok {
			next = map[string]any{}
			u.fields[parts[0]] = next
		}
		return (&Unstructured{fields: next}).SetField(parts[1], value)
	}

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
