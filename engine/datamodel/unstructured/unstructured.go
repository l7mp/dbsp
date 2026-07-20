package unstructured

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/ohler55/ojg/jp"

	"github.com/l7mp/dbsp/engine/datamodel"
)

// Unstructured implements datamodel.Document over a map[string]any (JSON object).
// Fields are stored as a plain Go map and serialised as a JSON object.
type Unstructured struct {
	fields map[string]any
}

// Ensure Unstructured implements datamodel.Document.
var _ datamodel.Document = (*Unstructured)(nil)

// New creates a new Unstructured document. The provided fields map is deep
// copied so that subsequent mutations of the caller's map do not affect the
// document.
func New(fields map[string]any) *Unstructured {
	f := make(map[string]any, len(fields))
	for k, v := range fields {
		f[k] = deepCopyAny(v)
	}
	return &Unstructured{fields: f}
}

// Wrap creates an Unstructured document sharing the provided fields map —
// no copy is made, so mutations flow both ways. This adapts an existing
// plain-map value to the Document interface (the expression engine wraps
// plain-map subjects this way, so subject paths resolve exactly like
// document paths); use New when the document must own its fields.
func Wrap(fields map[string]any) *Unstructured {
	return &Unstructured{fields: fields}
}

// Merge combines two unstructured documents, with right side overwriting key collisions.
func Merge(left, right *Unstructured) *Unstructured {
	if left == nil {
		if right == nil {
			return New(map[string]any{})
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

// String returns the canonical JSON representation of the document.
func (u *Unstructured) String() string {
	b, err := json.Marshal(u.fields)
	if err != nil {
		return fmt.Sprintf("%v", u.fields)
	}
	return string(b)
}

// Copy returns a deep copy of the document. Nested map[string]any and []any
// values are recursively cloned so that mutations of the copy never alias
// back to the original.
func (u *Unstructured) Copy() datamodel.Document {
	cp := &Unstructured{
		fields: make(map[string]any, len(u.fields)),
	}
	for k, v := range u.fields {
		cp.fields[k] = deepCopyAny(v)
	}
	return cp
}

// New returns a new empty Unstructured document.
func (u *Unstructured) New() datamodel.Document {
	return &Unstructured{
		fields: make(map[string]any),
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

// GetField returns the value for a field path. There is exactly one path
// semantics — "$"-rooted JSONPath, evaluated by the standard evaluator; a
// path without the "$" root is an error, not a bare field name. Map keys
// containing dots (Kubernetes label, annotation and Secret data keys) are
// addressed with bracket syntax: $["data"]["tls.crt"] — a dot in a path
// always traverses.
// It returns datamodel.ErrFieldNotFound when the path resolves to nothing.
func (u *Unstructured) GetField(key string) (any, error) {
	expr, err := parsePath(key)
	if err != nil {
		return nil, err
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

// parsedPaths caches compiled JSONPath expressions. Field paths come from
// compiled expression programs, so the live set is small and hot; jp.Expr
// values are read-only during Get/Set and safe to share.
var parsedPaths sync.Map // string -> jp.Expr

// parsePath compiles a field path to a JSONPath expression, caching the
// result. The path must be "$"-rooted: constructing the path (with bracket
// quoting for literal keys) is the caller's business, and silently reading
// a bare string as a path would blur the literal/path distinction.
func parsePath(key string) (jp.Expr, error) {
	if !strings.HasPrefix(key, "$") {
		return nil, fmt.Errorf("field path %q is not a $-rooted JSONPath", key)
	}
	if cached, ok := parsedPaths.Load(key); ok {
		return cached.(jp.Expr), nil
	}
	expr, err := jp.ParseString(key)
	if err != nil {
		return nil, fmt.Errorf("invalid JSONPath %q: %w", key, err)
	}
	parsedPaths.Store(key, expr)
	return expr, nil
}

// SetField sets the value for a field path, resolved with the same JSONPath
// semantics as GetField; missing intermediate maps are created.
func (u *Unstructured) SetField(key string, value any) error {
	expr, err := parsePath(key)
	if err != nil {
		return err
	}
	if err := expr.Set(u.fields, value); err != nil {
		return fmt.Errorf("set JSONPath %q: %w", key, err)
	}
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
