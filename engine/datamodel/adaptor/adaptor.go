package adaptor

import (
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

// TransformFunc transforms values on get/set.
type TransformFunc func(path string, value any) (any, error)

// Adaptor wraps a document and applies path-based transforms.
type Adaptor struct {
	base datamodel.Document
	in   TransformFunc
	out  TransformFunc
}

var _ datamodel.Document = (*Adaptor)(nil)

func New(base datamodel.Document, in, out TransformFunc) *Adaptor {
	return &Adaptor{base: base, in: in, out: out}
}

func (a *Adaptor) Hash() string { return a.base.Hash() }

func (a *Adaptor) PrimaryKey() (string, error) { return a.base.PrimaryKey() }

func (a *Adaptor) String() string { return a.base.String() }

func (a *Adaptor) Merge(other datamodel.Document) datamodel.Document {
	b, ok := other.(*Adaptor)
	if ok {
		return &Adaptor{base: a.base.Merge(b.base), in: a.in, out: a.out}
	}
	return &Adaptor{base: a.base.Merge(other), in: a.in, out: a.out}
}

func (a *Adaptor) Copy() datamodel.Document {
	return &Adaptor{base: a.base.Copy(), in: a.in, out: a.out}
}

func (a *Adaptor) New() datamodel.Document {
	return &Adaptor{base: a.base.New(), in: a.in, out: a.out}
}

func (a *Adaptor) GetField(key string) (any, error) {
	v, err := a.base.GetField(key)
	if err != nil {
		return nil, err
	}
	if a.in == nil {
		return v, nil
	}
	return a.in(key, v)
}

func (a *Adaptor) SetField(key string, value any) error {
	if a.out != nil {
		v, err := a.out(key, value)
		if err != nil {
			return err
		}
		value = v
	}
	return a.base.SetField(key, value)
}

func (a *Adaptor) MarshalJSON() ([]byte, error) { return a.base.MarshalJSON() }

func (a *Adaptor) UnmarshalJSON(data []byte) error {
	if a.base == nil {
		a.base = unstructured.New(map[string]any{}, nil)
	}
	return a.base.UnmarshalJSON(data)
}
