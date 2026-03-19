package operator

import (
	"fmt"
	"sort"

	"github.com/l7mp/dbsp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	"github.com/l7mp/dbsp/dbsp/expression"
	exprdbsp "github.com/l7mp/dbsp/dbsp/expression/dbsp"
	"github.com/l7mp/dbsp/dbsp/zset"
)

// Distinct converts Z-set to set (all positive weights become 1).
type Distinct struct{ nonLinearOp }

// NewDistinct creates a new Distinct operator.
func NewDistinct(opts ...Option) *Distinct {
	return &Distinct{newNonLinearOp(KindDistinct, 1, "Distinct", opts)}
}

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

// Aggregate is a generic self-contained incremental GROUP BY aggregate.
// It computes one output row per affected primary key using expression-based
// key/value extraction and reduce logic.
type Aggregate struct {
	nonLinearOp
	state      keyedState
	keyExpr    expression.Expression
	valueExpr  expression.Expression
	reduceExpr expression.Expression
	setExpr    expression.Expression
	outField   string
}

// NewAggregate creates a generic aggregate.
func NewAggregate(keyExpr, valueExpr, reduceExpr expression.Expression, outField string, opts ...Option) *Aggregate {
	if valueExpr == nil {
		valueExpr = expression.Func(func(ctx *expression.EvalContext) (any, error) {
			return ctx.Subject(), nil
		})
	}
	if reduceExpr == nil {
		reduceExpr = expression.Func(func(ctx *expression.EvalContext) (any, error) {
			return ctx.Subject(), nil
		})
	}
	if outField == "" {
		outField = "value"
	}
	return &Aggregate{
		nonLinearOp: newNonLinearOp(KindAggregate, 1, "Aggregate", opts),
		state: keyedState{
			pkIndex: map[string]map[string]datamodel.Document{},
			weights: map[string]zset.Weight{},
		},
		keyExpr:    keyExpr,
		valueExpr:  valueExpr,
		reduceExpr: reduceExpr,
		setExpr:    nil,
		outField:   outField,
	}
}

// Set initializes Aggregate state from v.
func (o *Aggregate) Set(v zset.ZSet) {
	o.state.resetFrom(v)
}

// Apply implements Operator.
func (o *Aggregate) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	reducer := aggregateReducer{
		keyExpr:    o.keyExpr,
		valueExpr:  o.valueExpr,
		reduceExpr: o.reduceExpr,
		setExpr:    o.setExpr,
		outField:   o.outField,
	}
	result, err := o.state.applyDelta(inputs[0], reducer)
	if err != nil {
		return zset.New(), err
	}
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

// NewAggregateWithSet creates a keyed aggregate that preserves a
// representative input document and applies setExpr to write the reduced value.
// In setExpr evaluation, Document() is the representative document and Subject()
// is the reduced value.
func NewAggregateWithSet(keyExpr, valueExpr, reduceExpr, setExpr expression.Expression, opts ...Option) *Aggregate {
	op := NewAggregate(keyExpr, valueExpr, reduceExpr, "", opts...)
	op.setExpr = setExpr
	op.display = "Aggregate(set)"
	return op
}

// NewDistinctPi returns a generic aggregate operator equivalent to the distinct_π semantics
func NewDistinctPi(opts ...Option) *Aggregate {
	op := NewAggregate(nil, nil, exprdbsp.NewLexMin(expression.Func(func(ctx *expression.EvalContext) (any, error) {
		return ctx.Subject(), nil
	})), "value", opts...)
	op.display = "Aggregate(distinct_pi)"
	return op
}

type keyedState struct {
	pkIndex map[string]map[string]datamodel.Document // pk → hash → Document (positive-weight only)
	weights map[string]zset.Weight                   // hash → accumulated weight
}

func (s *keyedState) ensure() {
	if s.pkIndex == nil {
		s.pkIndex = map[string]map[string]datamodel.Document{}
	}
	if s.weights == nil {
		s.weights = map[string]zset.Weight{}
	}
}

func (s *keyedState) resetFrom(v zset.ZSet) {
	s.pkIndex = map[string]map[string]datamodel.Document{}
	s.weights = map[string]zset.Weight{}
	if v.Size() == 0 {
		return
	}
	v.Iter(func(elem datamodel.Document, w zset.Weight) bool {
		h := elem.Hash()
		s.weights[h] = w
		if w > 0 {
			pk, _ := elem.PrimaryKey()
			if s.pkIndex[pk] == nil {
				s.pkIndex[pk] = map[string]datamodel.Document{}
			}
			s.pkIndex[pk][h] = elem
		}
		return true
	})
}

type keyedReducer interface {
	before(pk string, docs map[string]datamodel.Document, weights map[string]zset.Weight) (datamodel.Document, error)
	after(pk string, docs map[string]datamodel.Document, weights map[string]zset.Weight) (datamodel.Document, error)
}

func (s *keyedState) applyDelta(delta zset.ZSet, reducer keyedReducer) (zset.ZSet, error) {
	s.ensure()

	affectedPKs := map[string]struct{}{}
	var iterErr error
	delta.Iter(func(elem datamodel.Document, _ zset.Weight) bool {
		pk, err := elem.PrimaryKey()
		if err != nil {
			iterErr = fmt.Errorf("keyed reducer: PrimaryKey: %w", err)
			return false
		}
		affectedPKs[pk] = struct{}{}
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}

	result := zset.New()
	keys := make([]string, 0, len(affectedPKs))
	for pk := range affectedPKs {
		keys = append(keys, pk)
	}
	sort.Strings(keys)

	for _, pk := range keys {
		oldRow, err := reducer.before(pk, s.pkIndex[pk], s.weights)
		if err != nil {
			return zset.New(), err
		}

		delta.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
			epk, err := elem.PrimaryKey()
			if err != nil || epk != pk {
				return true
			}
			h := elem.Hash()
			oldWeight := s.weights[h]
			newWeight := oldWeight + weight
			s.weights[h] = newWeight
			if oldWeight <= 0 && newWeight > 0 {
				if s.pkIndex[pk] == nil {
					s.pkIndex[pk] = map[string]datamodel.Document{}
				}
				s.pkIndex[pk][h] = elem
			} else if oldWeight > 0 && newWeight <= 0 {
				delete(s.pkIndex[pk], h)
				if len(s.pkIndex[pk]) == 0 {
					delete(s.pkIndex, pk)
				}
			}
			return true
		})

		newRow, err := reducer.after(pk, s.pkIndex[pk], s.weights)
		if err != nil {
			return zset.New(), err
		}

		if oldRow != nil && (newRow == nil || oldRow.Hash() != newRow.Hash()) {
			result.Insert(oldRow, -1)
		}
		if newRow != nil && (oldRow == nil || oldRow.Hash() != newRow.Hash()) {
			result.Insert(newRow, 1)
		}
	}

	return result, nil
}

type aggregateReducer struct {
	keyExpr    expression.Expression
	valueExpr  expression.Expression
	reduceExpr expression.Expression
	setExpr    expression.Expression
	outField   string
}

func (r aggregateReducer) before(pk string, docs map[string]datamodel.Document, weights map[string]zset.Weight) (datamodel.Document, error) {
	return r.row(pk, docs, weights)
}

func (r aggregateReducer) after(pk string, docs map[string]datamodel.Document, weights map[string]zset.Weight) (datamodel.Document, error) {
	return r.row(pk, docs, weights)
}

func (r aggregateReducer) row(pk string, docs map[string]datamodel.Document, weights map[string]zset.Weight) (datamodel.Document, error) {
	if len(docs) == 0 {
		return nil, nil
	}

	values := make([]any, 0)
	for h, doc := range docs {
		w := weights[h]
		if w <= 0 {
			continue
		}
		v, err := r.valueExpr.Evaluate(expression.NewContext(doc).WithSubject(doc))
		if err != nil {
			return nil, fmt.Errorf("aggregate keyed: value expr: %w", err)
		}
		for i := zset.Weight(0); i < w; i++ {
			values = append(values, v)
		}
	}

	if len(values) == 0 {
		return nil, nil
	}

	keyDoc := representativeDoc(docs)
	if keyDoc == nil {
		return nil, nil
	}

	var keyValue any
	if r.keyExpr == nil {
		keyValue = pk
	} else {
		var err error
		keyValue, err = r.keyExpr.Evaluate(expression.NewContext(keyDoc).WithSubject(keyDoc))
		if err != nil {
			return nil, fmt.Errorf("aggregate keyed: key expr: %w", err)
		}
	}

	outValue, err := r.reduceExpr.Evaluate(expression.NewContext(nil).WithSubject(values))
	if err != nil {
		if len(values) == 1 {
			if list, lerr := exprdbsp.AsList(values[0]); lerr == nil {
				outValue, err = r.reduceExpr.Evaluate(expression.NewContext(nil).WithSubject(list))
				if err == nil {
					goto reduced
				}
			}
		}
		return nil, fmt.Errorf("aggregate keyed: reduce expr: %w", err)
	}

reduced:

	if r.setExpr != nil {
		setValue, err := r.setExpr.Evaluate(expression.NewContext(keyDoc).WithSubject(outValue))
		if err != nil {
			return nil, fmt.Errorf("aggregate keyed: set expr: %w", err)
		}
		doc, ok := setValue.(datamodel.Document)
		if !ok {
			return nil, fmt.Errorf("aggregate keyed: set expr must return a document, got %T", setValue)
		}
		return doc, nil
	}

	return unstructured.New(map[string]any{"key": keyValue, r.outField: outValue}, nil), nil
}

func representativeDoc(docs map[string]datamodel.Document) datamodel.Document {
	var rep datamodel.Document
	for h, doc := range docs {
		if rep == nil || h < rep.Hash() {
			rep = doc
		}
	}
	return rep
}
