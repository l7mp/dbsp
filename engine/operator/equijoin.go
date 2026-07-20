package operator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/zset"
)

// EquiJoin computes the key-equality join of two namespaced inputs: the
// output contains l.Merge(r) with weight lw·rw for every pair whose join
// keys are equal. The keys are arbitrary expressions evaluated against one
// named sub-document (namespace) of each side, so the operator slots
// directly into the compiler's namespaced join pipelines. Elements whose
// namespace or key does not resolve simply never match, mirroring the
// field-not-found-is-false convention of join predicates.
//
// EquiJoin is the indexed replacement for a CartesianProduct followed by an
// equality Select: one application costs O(|A| + |B| + matches) instead of
// O(|A|·|B|). Incrementalization replaces it with the stateful EquiJoinH,
// which keeps both sides indexed across steps and processes deltas in
// O(|Δ|·matches).
type EquiJoin struct {
	baseOp
	leftNS   string
	rightNS  string
	leftKey  expression.Expression
	rightKey expression.Expression
}

// NewEquiJoin creates an equi-join keyed by keyExpr-over-namespace on each
// side.
func NewEquiJoin(leftNS string, leftKey expression.Expression, rightNS string, rightKey expression.Expression, opts ...Option) *EquiJoin {
	return &EquiJoin{
		baseOp:   newBaseOp("equi_join", opts),
		leftNS:   leftNS,
		rightNS:  rightNS,
		leftKey:  leftKey,
		rightKey: rightKey,
	}
}

func (o *EquiJoin) Kind() Kind           { return KindEquiJoin }
func (o *EquiJoin) Arity() int           { return 2 }
func (o *EquiJoin) Linearity() Linearity { return NonLinear }
func (o *EquiJoin) String() string {
	return fmt.Sprintf("EquiJoin(%s ⋈ %s)", o.leftNS, o.rightNS)
}

// Incremental returns the stateful incremental replacement.
func (o *EquiJoin) Incremental() *EquiJoinH {
	return &EquiJoinH{
		baseOp:   newBaseOp("equi_join_incremental", nil),
		leftNS:   o.leftNS,
		rightNS:  o.rightNS,
		leftKey:  o.leftKey,
		rightKey: o.rightKey,
		left:     equiJoinIndex{},
		right:    equiJoinIndex{},
	}
}

// MarshalJSON implements json.Marshaler.
func (o *EquiJoin) MarshalJSON() ([]byte, error) {
	return marshalEquiJoin("equi_join", o.leftNS, o.rightNS, o.leftKey, o.rightKey)
}

// MarshalJSON implements json.Marshaler.
func (o *EquiJoinH) MarshalJSON() ([]byte, error) {
	return marshalEquiJoin("equi_join_incremental", o.leftNS, o.rightNS, o.leftKey, o.rightKey)
}

func marshalEquiJoin(typ, leftNS, rightNS string, leftKey, rightKey expression.Expression) ([]byte, error) {
	leftJSON, err := json.Marshal(leftKey)
	if err != nil {
		return nil, fmt.Errorf("marshal equi-join left key: %w", err)
	}
	rightJSON, err := json.Marshal(rightKey)
	if err != nil {
		return nil, fmt.Errorf("marshal equi-join right key: %w", err)
	}
	return json.Marshal(jsonOp{
		Type:     typ,
		LeftNS:   leftNS,
		RightNS:  rightNS,
		LeftKey:  leftJSON,
		RightKey: rightJSON,
	})
}

// joinKey evaluates the join key of one element: the namespace sub-document
// is extracted, the key expression is evaluated against it, and the value is
// canonically hashed. ok is false when the element cannot participate in the
// join (missing namespace or unresolvable key).
func joinKey(doc datamodel.Document, ns string, keyExpr expression.Expression) (string, bool, error) {
	sub, err := doc.GetField("$." + ns)
	if err != nil {
		if errors.Is(err, datamodel.ErrFieldNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	var subDoc datamodel.Document
	switch v := sub.(type) {
	case datamodel.Document:
		// Product namespaces hold their parts as documents.
		subDoc = v
	case map[string]any:
		subDoc = unstructured.New(v)
	default:
		return "", false, nil
	}

	v, err := keyExpr.Evaluate(expression.NewContext(subDoc))
	if err != nil {
		if errors.Is(err, datamodel.ErrFieldNotFound) {
			return "", false, nil
		}
		return "", false, err
	}

	data, err := json.Marshal(v)
	if err != nil {
		return "", false, fmt.Errorf("equi-join: cannot serialize key: %w", err)
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:12]), true, nil
}

// Apply implements Operator: a per-step hash join of the full inputs.
func (o *EquiJoin) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	left, right := inputs[0], inputs[1]
	result := zset.New()

	// Index the right side, probe with the left.
	index := map[string][]zset.Elem{}
	var applyErr error
	right.Iter(func(r datamodel.Document, rw zset.Weight) bool {
		key, ok, err := joinKey(r, o.rightNS, o.rightKey)
		if err != nil {
			applyErr = err
			return false
		}
		if ok {
			index[key] = append(index[key], zset.Elem{Document: r, Weight: rw})
		}
		return true
	})
	if applyErr != nil {
		return zset.New(), applyErr
	}

	left.Iter(func(l datamodel.Document, lw zset.Weight) bool {
		key, ok, err := joinKey(l, o.leftNS, o.leftKey)
		if err != nil {
			applyErr = err
			return false
		}
		if !ok {
			return true
		}
		for _, e := range index[key] {
			result.Insert(l.Merge(e.Document), lw*e.Weight)
		}
		return true
	})
	if applyErr != nil {
		return zset.New(), applyErr
	}

	if o.logger.V(2).Enabled() {
		o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	}
	return result, nil
}

// equiJoinIndex is a persistent one-side join state: join key hash -> element hash ->
// element. Weights sum on insertion and zero-weight entries vanish, exactly like a Z-set, so the
// index is the indexed integral of the side.
type equiJoinIndex map[string]map[string]zset.Elem

func (ix equiJoinIndex) insert(key string, doc datamodel.Document, w zset.Weight) {
	bucket, ok := ix[key]
	if !ok {
		bucket = map[string]zset.Elem{}
		ix[key] = bucket
	}
	h := doc.Hash()
	e, ok := bucket[h]
	if !ok {
		bucket[h] = zset.Elem{Document: doc, Weight: w}
		return
	}
	e.Weight += w
	if e.Weight == 0 {
		delete(bucket, h)
		if len(bucket) == 0 {
			delete(ix, key)
		}
		return
	}
	bucket[h] = e
}

// EquiJoinH is the stateful incremental form of EquiJoin, emitted by
// incrementalization. It maintains both sides indexed by join key and
// computes the standard bilinear delta
//
//	Δ(a ⋈ b) = Δa ⋈ B⁻ + A⁻ ⋈ Δb + Δa ⋈ Δb
//
// with the pre-step integrals A⁻/B⁻ read from (and then updated in) the
// internal indexes. Every term is evaluated by key lookup, so one step costs
// O(|Δ|·matches) regardless of the accumulated state size.
type EquiJoinH struct {
	baseOp
	leftNS   string
	rightNS  string
	leftKey  expression.Expression
	rightKey expression.Expression
	left     equiJoinIndex
	right    equiJoinIndex
}

func (o *EquiJoinH) Kind() Kind           { return KindEquiJoinH }
func (o *EquiJoinH) Arity() int           { return 2 }
func (o *EquiJoinH) Linearity() Linearity { return Primitive }
func (o *EquiJoinH) String() string {
	return fmt.Sprintf("EquiJoinH(%s ⋈ %s)", o.leftNS, o.rightNS)
}

// Set resets the join state. A non-empty snapshot cannot be attributed to
// one side, so any content is discarded: only the zero reset is meaningful.
func (o *EquiJoinH) Set(_ zset.ZSet) {
	o.left = equiJoinIndex{}
	o.right = equiJoinIndex{}
}

// Apply implements Operator.
func (o *EquiJoinH) Apply(_ *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	dl, dr := inputs[0], inputs[1]
	result := zset.New()

	// Δa ⋈ B⁻ and Δa ⋈ Δb (the right delta is indexed on the fly).
	drIndex := equiJoinIndex{}
	var applyErr error
	dr.Iter(func(r datamodel.Document, rw zset.Weight) bool {
		key, ok, err := joinKey(r, o.rightNS, o.rightKey)
		if err != nil {
			applyErr = err
			return false
		}
		if ok {
			drIndex.insert(key, r, rw)
		}
		return true
	})
	if applyErr != nil {
		return zset.New(), applyErr
	}

	dl.Iter(func(l datamodel.Document, lw zset.Weight) bool {
		key, ok, err := joinKey(l, o.leftNS, o.leftKey)
		if err != nil {
			applyErr = err
			return false
		}
		if !ok {
			return true
		}
		for _, e := range o.right[key] {
			result.Insert(l.Merge(e.Document), lw*e.Weight)
		}
		for _, e := range drIndex[key] {
			result.Insert(l.Merge(e.Document), lw*e.Weight)
		}
		return true
	})
	if applyErr != nil {
		return zset.New(), applyErr
	}

	// A⁻ ⋈ Δb.
	dr.Iter(func(r datamodel.Document, rw zset.Weight) bool {
		key, ok, err := joinKey(r, o.rightNS, o.rightKey)
		if err != nil {
			applyErr = err
			return false
		}
		if !ok {
			return true
		}
		for _, e := range o.left[key] {
			result.Insert(e.Document.Merge(r), e.Weight*rw)
		}
		return true
	})
	if applyErr != nil {
		return zset.New(), applyErr
	}

	// Fold the deltas into the indexes.
	dl.Iter(func(l datamodel.Document, lw zset.Weight) bool {
		key, ok, err := joinKey(l, o.leftNS, o.leftKey)
		if err != nil {
			applyErr = err
			return false
		}
		if ok {
			o.left.insert(key, l, lw)
		}
		return true
	})
	if applyErr != nil {
		return zset.New(), applyErr
	}
	for key, bucket := range drIndex {
		for _, e := range bucket {
			o.right.insert(key, e.Document, e.Weight)
		}
	}

	if o.logger.V(2).Enabled() {
		o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	}
	return result, nil
}
