package operator

import (
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/zset"
)

// CartesianProduct computes A x B using document Merge.
type CartesianProduct struct{ bilinearOp }

// NewCartesianProduct creates a new CartesianProduct operator.
func NewCartesianProduct(opts ...Option) *CartesianProduct {
	return &CartesianProduct{newBilinearOp(KindCartesian, 2, "×", opts)}
}

// Apply implements Operator.
func (o *CartesianProduct) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	left, right := inputs[0], inputs[1]
	result := zset.New()

	left.Iter(func(l datamodel.Document, lw zset.Weight) bool {
		right.Iter(func(r datamodel.Document, rw zset.Weight) bool {
			result.Insert(l.Merge(r), lw*rw)
			return true
		})
		return true
	})

	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}
