package product

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

// Product is a namespaced document made of named sub-documents.
type Product struct {
	parts map[string]datamodel.Document
}

var _ datamodel.Document = (*Product)(nil)

func New(parts map[string]datamodel.Document) *Product {
	cp := make(map[string]datamodel.Document, len(parts))
	for k, v := range parts {
		if v == nil {
			continue
		}
		cp[k] = v.Copy()
	}
	return &Product{parts: cp}
}

func (p *Product) Hash() string {
	keys := make([]string, 0, len(p.parts))
	for k := range p.parts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	chunks := make([]string, 0, len(keys))
	for _, k := range keys {
		chunks = append(chunks, fmt.Sprintf("%s=%s", k, p.parts[k].Hash()))
	}
	return strings.Join(chunks, "|")
}

func (p *Product) PrimaryKey() (string, error) { return p.Hash(), nil }

func (p *Product) String() string { return p.Hash() }

func (p *Product) Copy() datamodel.Document { return New(p.parts) }

func (p *Product) Merge(other datamodel.Document) datamodel.Document {
	op, ok := other.(*Product)
	if !ok {
		return p.Copy()
	}
	parts := make(map[string]datamodel.Document, len(p.parts)+len(op.parts))
	for k, v := range p.parts {
		parts[k] = v.Copy()
	}
	for k, v := range op.parts {
		parts[k] = v.Copy()
	}
	return &Product{parts: parts}
}

func (p *Product) New() datamodel.Document { return &Product{parts: map[string]datamodel.Document{}} }

func (p *Product) GetField(key string) (any, error) {
	if key == "$" || key == "$." {
		root := map[string]any{}
		for k, v := range p.parts {
			root[k] = v
		}
		return root, nil
	}
	k := strings.TrimPrefix(key, "$.")
	parts := strings.SplitN(k, ".", 2)
	d, ok := p.parts[parts[0]]
	if !ok {
		return nil, fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, key)
	}
	if len(parts) == 1 {
		return d, nil
	}
	return d.GetField(parts[1])
}

func (p *Product) SetField(key string, value any) error {
	k := strings.TrimPrefix(key, "$.")
	parts := strings.SplitN(k, ".", 2)
	if len(parts) == 1 {
		d, ok := value.(datamodel.Document)
		if !ok {
			return fmt.Errorf("set part %q: expected datamodel.Document, got %T", parts[0], value)
		}
		p.parts[parts[0]] = d.Copy()
		return nil
	}
	d, ok := p.parts[parts[0]]
	if !ok {
		return fmt.Errorf("%w: %s", datamodel.ErrFieldNotFound, parts[0])
	}
	return d.SetField(parts[1], value)
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
		u := unstructured.New(map[string]any{}, nil)
		if err := u.UnmarshalJSON(blob); err != nil {
			return err
		}
		parts[k] = u
	}
	p.parts = parts
	return nil
}
