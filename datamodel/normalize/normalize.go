package normalize

import (
	"fmt"

	"github.com/l7mp/dbsp/datamodel"
)

// Transformer mutates a document in-place during normalization.
type Transformer interface {
	Transform(doc datamodel.Document) error
}

// TransformerFunc adapts a function to a Transformer.
type TransformerFunc func(doc datamodel.Document) error

// Transform implements Transformer.
func (f TransformerFunc) Transform(doc datamodel.Document) error { return f(doc) }

// Chain applies transformers in order on a copy of the input document.
type Chain struct {
	steps []Transformer
}

// NewChain creates a new normalization chain.
func NewChain(steps ...Transformer) Chain {
	return Chain{steps: append([]Transformer(nil), steps...)}
}

// Normalize applies all transformers to a deep copy of doc.
func (c Chain) Normalize(doc datamodel.Document) (datamodel.Document, error) {
	if doc == nil {
		return nil, nil
	}
	out := doc.Copy()
	for i, step := range c.steps {
		if err := step.Transform(out); err != nil {
			return nil, fmt.Errorf("normalize step %d: %w", i, err)
		}
	}
	return out, nil
}
