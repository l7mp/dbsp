package operator

import (
	"fmt"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/internal/logger"
)

// Distinct converts Z-set to set (all positive weights become 1).
type Distinct struct {
	name   string
	logger logr.Logger
}

// NewDistinct creates a new Distinct operator.
func NewDistinct(name string, opts ...Option) *Distinct {
	o := &Distinct{name: name}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// Name implements Operator.
func (o *Distinct) Name() string { return o.name }

// String implements fmt.Stringer.
func (o *Distinct) String() string {
	return fmt.Sprintf("Distinct(%s)", o.name)
}

// Arity implements Operator.
func (o *Distinct) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Distinct) Linearity() Linearity { return NonLinear }

// Apply implements Operator.
func (o *Distinct) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	inputs[0].Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		if weight > 0 {
			result.Insert(elem, 1)
		}
		return true
	})
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

func (o *Distinct) setLogger(l logr.Logger) { o.logger = l }
