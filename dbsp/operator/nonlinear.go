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
	jsonUnsupported
	logger logr.Logger
}

// NewDistinct creates a new Distinct operator.
func NewDistinct(opts ...Option) *Distinct {
	o := &Distinct{}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// String implements fmt.Stringer.
func (o *Distinct) String() string {
	return "Distinct"
}

// Arity implements Operator.
func (o *Distinct) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Distinct) Linearity() Linearity { return NonLinear }
func (o *Distinct) Kind() Kind           { return KindDistinct }

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

func (o *Distinct) Set(_ zset.ZSet)         {}
func (o *Distinct) setLogger(l logr.Logger) { o.logger = l }

// DistinctKeyed is the SotW distinct_π operator. It selects at most one
// element per primary key from a Z-set: the lexicographic minimum by Hash()
// among all elements with positive weight.
type DistinctKeyed struct {
	jsonUnsupported
	logger logr.Logger
}

// NewDistinctKeyed creates a new DistinctKeyed operator.
func NewDistinctKeyed(opts ...Option) *DistinctKeyed {
	o := &DistinctKeyed{}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// String implements fmt.Stringer.
func (o *DistinctKeyed) String() string { return "DistinctKeyed" }

// Arity implements Operator.
func (o *DistinctKeyed) Arity() int { return 1 }

// Linearity implements Operator.
func (o *DistinctKeyed) Linearity() Linearity { return NonLinear }
func (o *DistinctKeyed) Kind() Kind           { return KindDistinctKeyed }

// Apply implements Operator. For each primary key, it inserts the element with
// the lexicographically smallest Hash() among all positive-weight elements.
func (o *DistinctKeyed) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	// Group positive-weight elements by primary key, tracking the lexmin per group.
	lexmin := map[string]datamodel.Document{} // pk → current lexmin document
	var iterErr error
	inputs[0].Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		if weight <= 0 {
			return true
		}
		pk, err := elem.PrimaryKey()
		if err != nil {
			iterErr = fmt.Errorf("DistinctKeyed: PrimaryKey: %w", err)
			return false
		}
		cur, exists := lexmin[pk]
		if !exists || elem.Hash() < cur.Hash() {
			lexmin[pk] = elem
		}
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}
	result := zset.New()
	for _, elem := range lexmin {
		result.Insert(elem, 1)
	}
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

func (o *DistinctKeyed) Set(_ zset.ZSet)         {}
func (o *DistinctKeyed) setLogger(l logr.Logger) { o.logger = l }

// HKeyed is the incremental distinct_π operator. It is self-contained: it
// maintains its own accumulated weight table (weights) and a secondary index
// (pkIndex) of the currently live elements per primary key.  No external
// integrator or delay is needed.
//
// Port 0: delta[t] — the current input delta.
//
// HKeyed is marked NonLinear so the incrementalizer does not attempt to wrap
// it again with D ∘ O ∘ I.
type HKeyed struct {
	jsonUnsupported
	pkIndex map[string]map[string]datamodel.Document // pk → hash → Document (positive-weight only)
	weights map[string]zset.Weight                   // hash → accumulated weight
	logger  logr.Logger
}

// NewHKeyed creates a new HKeyed operator.
func NewHKeyed(opts ...Option) *HKeyed {
	o := &HKeyed{}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// String implements fmt.Stringer.
func (o *HKeyed) String() string { return "HKeyed" }

// Arity implements Operator.
func (o *HKeyed) Arity() int { return 1 }

// Linearity implements Operator.
func (o *HKeyed) Linearity() Linearity { return NonLinear }
func (o *HKeyed) Kind() Kind           { return KindHKeyed }

// Set initializes HKeyed state from v. Set(zset.New()) clears all state.
// v is treated as the accumulated weight table: each element's weight is
// stored in weights and positive-weight elements are indexed by primary key.
func (o *HKeyed) Set(v zset.ZSet) {
	o.pkIndex = nil
	o.weights = nil
	if v.Size() == 0 {
		return
	}
	o.pkIndex = map[string]map[string]datamodel.Document{}
	o.weights = map[string]zset.Weight{}
	v.Iter(func(elem datamodel.Document, w zset.Weight) bool {
		h := elem.Hash()
		o.weights[h] = w
		if w > 0 {
			pk, _ := elem.PrimaryKey()
			if o.pkIndex[pk] == nil {
				o.pkIndex[pk] = map[string]datamodel.Document{}
			}
			o.pkIndex[pk][h] = elem
		}
		return true
	})
}

// Apply implements Operator.
func (o *HKeyed) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	delta := inputs[0]

	if o.pkIndex == nil {
		o.pkIndex = map[string]map[string]datamodel.Document{}
		o.weights = map[string]zset.Weight{}
	}

	// Collect the primary keys touched by this delta.
	affectedPKs := map[string]struct{}{}
	var iterErr error
	delta.Iter(func(elem datamodel.Document, _ zset.Weight) bool {
		pk, err := elem.PrimaryKey()
		if err != nil {
			iterErr = fmt.Errorf("HKeyed: PrimaryKey: %w", err)
			return false
		}
		affectedPKs[pk] = struct{}{}
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}

	// For each affected PK, record old lexmin, update weights and pkIndex, compute new lexmin.
	result := zset.New()
	for pk := range affectedPKs {
		oldMin := pkLexmin(o.pkIndex[pk])

		// Update weights and pkIndex for every element in delta with this PK.
		delta.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
			epk, err := elem.PrimaryKey()
			if err != nil || epk != pk {
				return true
			}
			h := elem.Hash()
			oldWeight := o.weights[h]
			newWeight := oldWeight + weight
			o.weights[h] = newWeight
			if oldWeight <= 0 && newWeight > 0 {
				if o.pkIndex[pk] == nil {
					o.pkIndex[pk] = map[string]datamodel.Document{}
				}
				o.pkIndex[pk][h] = elem
			} else if oldWeight > 0 && newWeight <= 0 {
				delete(o.pkIndex[pk], h)
				if len(o.pkIndex[pk]) == 0 {
					delete(o.pkIndex, pk)
				}
			}
			return true
		})

		newMin := pkLexmin(o.pkIndex[pk])

		if oldMin != nil && (newMin == nil || oldMin.Hash() != newMin.Hash()) {
			result.Insert(oldMin, -1)
		}
		if newMin != nil && (oldMin == nil || oldMin.Hash() != newMin.Hash()) {
			result.Insert(newMin, 1)
		}
	}

	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

func (o *HKeyed) setLogger(l logr.Logger) { o.logger = l }

// pkLexmin returns the lexicographically minimum document (by Hash) in the
// given map, or nil if the map is empty.
func pkLexmin(docs map[string]datamodel.Document) datamodel.Document {
	var min datamodel.Document
	for h, doc := range docs {
		if min == nil || h < min.Hash() {
			min = doc
		}
	}
	return min
}
