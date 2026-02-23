package operator

import (
	"fmt"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/internal/logger"
)

// CartesianProduct computes A x B using Document.Concat to combine elements.
type CartesianProduct struct {
	jsonUnsupported
	name   string
	logger logr.Logger
}

// NewCartesianProduct creates a new CartesianProduct operator.
func NewCartesianProduct(name string, opts ...Option) *CartesianProduct {
	o := &CartesianProduct{name: name}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// Name implements Operator.
func (o *CartesianProduct) Name() string { return o.name }

// String implements fmt.Stringer.
func (o *CartesianProduct) String() string {
	return fmt.Sprintf("CartesianProduct(%s)", o.name)
}

// Arity implements Operator.
func (o *CartesianProduct) Arity() int { return 2 }

// Linearity implements Operator.
func (o *CartesianProduct) Linearity() Linearity { return Bilinear }

// Apply implements Operator.
// It uses Document.Concat to combine left and right elements into a single flattened document.
func (o *CartesianProduct) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	left, right := inputs[0], inputs[1]
	result := zset.New()

	left.Iter(func(l datamodel.Document, lw zset.Weight) bool {
		right.Iter(func(r datamodel.Document, rw zset.Weight) bool {
			result.Insert(l.Concat(r), lw*rw)
			return true
		})
		return true
	})

	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

func (o *CartesianProduct) setLogger(l logr.Logger) { o.logger = l }
