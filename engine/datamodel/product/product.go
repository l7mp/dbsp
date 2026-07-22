package product

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/ohler55/ojg/jp"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

// Product is a namespaced document made of named sub-documents.
type Product struct {
	parts map[string]datamodel.Document

	// hash caches the content digest returned by Hash. An empty string
	// means the digest has not been computed since the last mutation; every
	// method that mutates parts must reset it.
	hash string
}

var _ datamodel.Document = (*Product)(nil)

func New(parts map[string]datamodel.Document) *Product {
	cp := make(map[string]datamodel.Document, len(parts))
	for k, v := range parts {
		if v == nil {
			cp[k] = nil
			continue
		}
		cp[k] = v.Copy()
	}
	return &Product{parts: cp}
}

// Hash returns the content digest of the product: an FNV-1a 128-bit hash of
// the canonical JSON serialization of the materialized object. Hashing the
// materialized content (not a composition of part digests) makes the digest
// representation-independent: a product hashes equal to an Unstructured
// document carrying the same namespaced content, so streams mixing the two
// representations compare correctly. The digest is cached and reset by the
// mutators.
func (p *Product) Hash() string {
	if p.hash != "" {
		return p.hash
	}
	h := fnv.New128a()
	h.Write([]byte(p.String()))
	p.hash = hex.EncodeToString(h.Sum(nil))
	return p.hash
}

// String returns the canonical JSON serialization of the materialized
// object (encoding/json marshals map keys in sorted order, and the parts
// serialize canonically, so the rendering is deterministic).
func (p *Product) String() string {
	b, err := p.MarshalJSON()
	if err != nil {
		return fmt.Sprintf("%v", p.parts)
	}
	return string(b)
}

func (p *Product) Copy() datamodel.Document { return New(p.parts) }

func (p *Product) Merge(other datamodel.Document) datamodel.Document {
	op, ok := other.(*Product)
	if !ok {
		return p.Copy()
	}
	parts := make(map[string]datamodel.Document, len(p.parts)+len(op.parts))
	for k, v := range p.parts {
		if v == nil {
			parts[k] = nil
			continue
		}
		parts[k] = v.Copy()
	}
	for k, v := range op.parts {
		if v == nil {
			parts[k] = nil
			continue
		}
		parts[k] = v.Copy()
	}
	return &Product{parts: parts}
}

func (p *Product) New() datamodel.Document { return &Product{parts: map[string]datamodel.Document{}} }

// GetField resolves a $-rooted JSONPath against the product: the first
// child fragment names the join input (the namespace), and the rest of the
// path delegates to that member document with the member evaluating its own
// share of the path.
func (p *Product) GetField(key string) (any, error) {
	if strings.HasPrefix(key, "$[") {
		return p.getJSONPath(key)
	}
	if key == "$" || key == "$." {
		root := map[string]any{}
		for k, v := range p.parts {
			root[k] = v
		}
		return root, nil
	}
	if !strings.HasPrefix(key, "$.") {
		return nil, fmt.Errorf("field path %q is not a $-rooted JSONPath", key)
	}
	parts := strings.SplitN(key[2:], ".", 2)
	d, ok := p.parts[parts[0]]
	if !ok {
		return nil, fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, key)
	}
	if len(parts) == 1 {
		return d, nil
	}
	if d == nil {
		return nil, fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, key)
	}
	return d.GetField("$." + parts[1])
}

func (p *Product) getJSONPath(key string) (any, error) {
	expr, err := jp.ParseString(key)
	if err != nil {
		return nil, fmt.Errorf("invalid JSONPath %q: %w", key, err)
	}

	root := map[string]any{}
	for k, v := range p.parts {
		if v == nil {
			root[k] = nil
			continue
		}
		root[k] = v.Fields()
	}

	results := expr.Get(root)
	if len(results) == 0 {
		return nil, fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, key)
	}
	if len(results) == 1 {
		return results[0], nil
	}

	return results, nil
}

func (p *Product) SetField(key string, value any) error {
	if !strings.HasPrefix(key, "$.") {
		return fmt.Errorf("field path %q is not a $-rooted JSONPath", key)
	}
	parts := strings.SplitN(key[2:], ".", 2)
	if len(parts) == 1 {
		d, ok := value.(datamodel.Document)
		if !ok {
			return fmt.Errorf("set part %q: expected datamodel.Document, got %T", parts[0], value)
		}
		p.parts[parts[0]] = d.Copy()
		p.hash = ""
		return nil
	}
	d, ok := p.parts[parts[0]]
	if !ok {
		return fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, parts[0])
	}
	p.hash = ""
	return d.SetField("$."+parts[1], value)
}

func (p *Product) Fields() map[string]any {
	fieldsMap := map[string]any{}
	for k, v := range p.parts {
		fieldsMap[k] = v
	}
	return fieldsMap
}

func (p *Product) MarshalJSON() ([]byte, error) {
	obj := map[string]any{}
	for k, v := range p.parts {
		obj[k] = v
	}
	return json.Marshal(obj)
}

func (p *Product) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parts := map[string]datamodel.Document{}
	for k, blob := range raw {
		u := unstructured.New(map[string]any{})
		if err := u.UnmarshalJSON(blob); err != nil {
			return err
		}
		parts[k] = u
	}
	p.parts = parts
	p.hash = ""
	return nil
}
