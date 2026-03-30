package operator

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/zset"
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

// GroupBy is a self-contained incremental grouping operator.
//
// For each affected key it emits at most two row-level deltas: remove old row
// with weight -1 and add new row with weight +1.
//
// Output row schema:
//   - key: grouping key value.
//   - values: list of valueExpr outputs.
//   - documents: list of full source documents.
//
// Bag semantics are the default: multiplicity follows input Z-set weights.
// With distinct=true duplicates are removed independently from values and
// documents.
type GroupBy struct {
	nonLinearOp
	state     groupState
	keyExpr   expression.Expression
	valueExpr expression.Expression
	distinct  bool
}

// NewGroupBy creates an incremental GROUP BY operator.
//
// keyExpr may be nil; in that case document primary key is used as group key.
// valueExpr may be nil; in that case the whole document is used as value.
func NewGroupBy(keyExpr, valueExpr expression.Expression, opts ...Option) *GroupBy {
	if valueExpr == nil {
		valueExpr = expression.Func(func(ctx *expression.EvalContext) (any, error) {
			return ctx.Subject(), nil
		})
	}
	return &GroupBy{
		nonLinearOp: newNonLinearOp(KindGroupBy, 1, "GroupBy", opts),
		state: groupState{
			groups: map[string]*groupBucket{},
		},
		keyExpr:   keyExpr,
		valueExpr: valueExpr,
	}
}

// WithDistinct toggles duplicate elimination in output lists.
func (o *GroupBy) WithDistinct(distinct bool) *GroupBy {
	o.distinct = distinct
	if distinct {
		o.display = "GroupBy(distinct)"
	} else {
		o.display = "GroupBy"
	}
	return o
}

// Set initializes GroupBy state from a full input snapshot.
func (o *GroupBy) Set(v zset.ZSet) {
	o.state = groupState{groups: map[string]*groupBucket{}}
	_, _ = o.state.applyDelta(v, o.keyExpr, o.valueExpr, o.distinct)
}

// Apply implements Operator.
func (o *GroupBy) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	if o.logger.V(2).Enabled() {
		o.logger.V(2).Info("dbsp runtime state",
			"event_type", "group_by.state.before",
			"component_type", "operator",
			"component", o.String(),
			"state", o.state.snapshot(),
		)
	}

	result, err := o.state.applyDelta(inputs[0], o.keyExpr, o.valueExpr, o.distinct)
	if err != nil {
		return zset.New(), err
	}

	if o.logger.V(2).Enabled() {
		o.logger.V(2).Info("dbsp runtime state",
			"event_type", "group_by.state.after",
			"component_type", "operator",
			"component", o.String(),
			"state", o.state.snapshot(),
			"delta", result.String(),
		)
	}
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

type groupBucket struct {
	key     any
	docs    map[string]datamodel.Document
	weights map[string]zset.Weight
}

type groupState struct {
	groups map[string]*groupBucket
}

func (s *groupState) ensure() {
	if s.groups == nil {
		s.groups = map[string]*groupBucket{}
	}
}

func (s *groupState) snapshot() []string {
	if s == nil || len(s.groups) == 0 {
		return []string{}
	}

	ids := make([]string, 0, len(s.groups))
	for id := range s.groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]string, 0)
	for _, id := range ids {
		bucket := s.groups[id]
		hashes := make([]string, 0, len(bucket.docs))
		for h := range bucket.docs {
			hashes = append(hashes, h)
		}
		sort.Strings(hashes)
		for _, h := range hashes {
			doc := bucket.docs[h]
			w := bucket.weights[h]
			out = append(out, fmt.Sprintf("group=%s hash=%s weight=%d doc=%s", id, h, w, doc.String()))
		}
	}

	return out
}

func (s *groupState) applyDelta(delta zset.ZSet, keyExpr, valueExpr expression.Expression, distinct bool) (zset.ZSet, error) {
	s.ensure()

	affected := map[string]struct{}{}
	var iterErr error

	delta.Iter(func(elem datamodel.Document, _ zset.Weight) bool {
		id, _, err := evaluateGroupKey(elem, keyExpr)
		if err != nil {
			iterErr = err
			return false
		}
		affected[id] = struct{}{}
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}

	oldRows := map[string]datamodel.Document{}
	ids := make([]string, 0, len(affected))
	for id := range affected {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		row, err := buildGroupRow(s.groups[id], valueExpr, distinct)
		if err != nil {
			return zset.New(), err
		}
		oldRows[id] = row
	}

	delta.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		id, key, err := evaluateGroupKey(elem, keyExpr)
		if err != nil {
			iterErr = err
			return false
		}

		bucket := s.groups[id]
		if bucket == nil {
			bucket = &groupBucket{
				key:     key,
				docs:    map[string]datamodel.Document{},
				weights: map[string]zset.Weight{},
			}
			s.groups[id] = bucket
		}

		h := elem.Hash()
		oldW := bucket.weights[h]
		newW := oldW + weight

		switch {
		case oldW <= 0 && newW > 0:
			bucket.docs[h] = elem
			bucket.weights[h] = newW
		case oldW > 0 && newW <= 0:
			delete(bucket.docs, h)
			delete(bucket.weights, h)
		case newW > 0:
			bucket.weights[h] = newW
		default:
			delete(bucket.weights, h)
		}

		if len(bucket.docs) == 0 {
			delete(s.groups, id)
		}
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}

	out := zset.New()
	for _, id := range ids {
		newRow, err := buildGroupRow(s.groups[id], valueExpr, distinct)
		if err != nil {
			return zset.New(), err
		}
		oldRow := oldRows[id]

		if oldRow != nil && (newRow == nil || oldRow.Hash() != newRow.Hash()) {
			out.Insert(oldRow, -1)
		}
		if newRow != nil && (oldRow == nil || oldRow.Hash() != newRow.Hash()) {
			out.Insert(newRow, 1)
		}
	}

	return out, nil
}

func evaluateGroupKey(doc datamodel.Document, keyExpr expression.Expression) (string, any, error) {
	var key any
	if keyExpr == nil {
		pk, err := doc.PrimaryKey()
		if err != nil {
			return "", nil, fmt.Errorf("group_by: primary key: %w", err)
		}
		key = pk
	} else {
		v, err := keyExpr.Evaluate(expression.NewContext(doc).WithSubject(doc))
		if err != nil {
			return "", nil, fmt.Errorf("group_by: key expr: %w", err)
		}
		key = v
	}

	id, err := digestKey(key)
	if err != nil {
		return "", nil, fmt.Errorf("group_by: key digest: %w", err)
	}

	return id, key, nil
}

func buildGroupRow(bucket *groupBucket, valueExpr expression.Expression, distinct bool) (datamodel.Document, error) {
	if bucket == nil || len(bucket.docs) == 0 {
		return nil, nil
	}

	hashes := make([]string, 0, len(bucket.docs))
	for h := range bucket.docs {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)

	values := make([]any, 0)
	documents := make([]any, 0)
	seenValues := map[string]struct{}{}
	seenDocs := map[string]struct{}{}

	for _, h := range hashes {
		doc := bucket.docs[h]
		w := bucket.weights[h]
		if w <= 0 {
			continue
		}

		value, err := valueExpr.Evaluate(expression.NewContext(doc).WithSubject(doc))
		if err != nil {
			return nil, fmt.Errorf("group_by: value expr: %w", err)
		}

		normDoc, err := normalizeAny(doc)
		if err != nil {
			return nil, fmt.Errorf("group_by: normalize document: %w", err)
		}

		for i := zset.Weight(0); i < w; i++ {
			if distinct {
				valueDigest, err := digestAny(value)
				if err != nil {
					return nil, fmt.Errorf("group_by: distinct value digest: %w", err)
				}
				if _, ok := seenValues[valueDigest]; !ok {
					values = append(values, value)
					seenValues[valueDigest] = struct{}{}
				}

				docDigest, err := digestAny(normDoc)
				if err != nil {
					return nil, fmt.Errorf("group_by: distinct document digest: %w", err)
				}
				if _, ok := seenDocs[docDigest]; !ok {
					documents = append(documents, normDoc)
					seenDocs[docDigest] = struct{}{}
				}
				continue
			}

			values = append(values, value)
			documents = append(documents, normDoc)
		}
	}

	if len(values) == 0 && len(documents) == 0 {
		return nil, nil
	}

	keyValue, err := normalizeAny(bucket.key)
	if err != nil {
		return nil, fmt.Errorf("group_by: normalize key: %w", err)
	}

	return unstructured.New(map[string]any{
		"key":       keyValue,
		"values":    values,
		"documents": documents,
	}, nil), nil
}

func digestKey(v any) (string, error) {
	norm, err := normalizeAny(v)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(norm)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func digestAny(v any) (string, error) {
	norm, err := normalizeAny(v)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(norm)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func normalizeAny(v any) (any, error) {
	if doc, ok := v.(datamodel.Document); ok {
		b, err := doc.MarshalJSON()
		if err != nil {
			return nil, err
		}
		var out any
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	return v, nil
}
