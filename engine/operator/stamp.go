package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/expression"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/zset"
)

// The stamp operators fill assign-once fields: values that depend on when
// a document appeared rather than on what the current input is (a
// condition's lastTransitionTime, a generated name or port). The contract
// is the FRP hold(snapshot(...)) pattern: the stamped value of a document
// is the value sampled when the document's key first appeared, held
// unchanged for as long as the key is live, and forgotten when the key's
// weight drains to zero. Everything about what counts as a transition is
// in the key: keying a condition on (object, type, status) restamps on a
// status flip and holds across reason edits, keying on (object) stamps
// once per object lifetime.
//
// Both forms are stateful: the held values are a function of the input
// history, so no lifted function of the current level computes them. The
// pair satisfies Stamp = ∫ ∘ StampIncremental ∘ D by construction, which
// makes the generic D ∘ Stamp ∘ ∫ incrementalization correct and the
// dedicated StampIncremental substitution an optimization.

// stampField is one target path with the expression that samples it.
type stampField struct {
	path string
	expr expression.Expression // nil: hold the value the document carries
}

// stampEntry is the held state of one live key: the sampled values and,
// in the delta form, the integrated weight of every emitted row of the key
// by the stamped row's hash. A key is live while any of its rows has a
// non-zero weight; the per-row weights are what make that test exact
// under negative weights (a row in debt does not drain a key whose other
// rows are present).
type stampEntry struct {
	key     any
	values  map[string]any         // field path -> held value
	weights map[string]zset.Weight // delta form only: stamped row hash -> weight
}

// Stamp is the snapshot form: a stateful level processor. A row whose key
// was present in the previous step's level keeps the held values; a new
// key samples the value expressions with the frozen round clock, once for
// all its rows this step. Keys absent from the level are forgotten, so a
// reappearing key samples afresh.
type Stamp struct {
	nonLinearOp
	keyExpr expression.Expression
	fields  []stampField // sorted by path
	state   map[string]*stampEntry
}

// NewStamp creates a snapshot stamp operator. keyExpr is evaluated against
// the unstamped document; fields maps target paths to value expressions,
// where a nil expression holds whatever value the document carries.
func NewStamp(keyExpr expression.Expression, fields map[string]expression.Expression, opts ...Option) *Stamp {
	return &Stamp{
		nonLinearOp: newNonLinearOp(KindStamp, 1, "Stamp", opts),
		keyExpr:     keyExpr,
		fields:      sortStampFields(fields),
		state:       map[string]*stampEntry{},
	}
}

func sortStampFields(fields map[string]expression.Expression) []stampField {
	fs := make([]stampField, 0, len(fields))
	for p, e := range fields {
		fs = append(fs, stampField{path: p, expr: e})
	}
	sort.Slice(fs, func(i, j int) bool { return fs[i].path < fs[j].path })
	return fs
}

// KeyExpr returns the key expression.
func (o *Stamp) KeyExpr() expression.Expression { return o.keyExpr }

// Fields returns the stamped paths with their value expressions.
func (o *Stamp) Fields() map[string]expression.Expression {
	out := make(map[string]expression.Expression, len(o.fields))
	for _, f := range o.fields {
		out[f.path] = f.expr
	}
	return out
}

// Incremental returns the delta form sharing the key and field
// expressions, with fresh state.
func (o *Stamp) Incremental() *StampIncremental {
	return &StampIncremental{
		baseOp:  newBaseOp("stamp_incremental", nil),
		keyExpr: o.keyExpr,
		fields:  o.fields,
		state:   map[string]*stampEntry{},
	}
}

// Set seeds the held values from a level of already-stamped rows (the
// observed objects at startup, say). Rows missing a stamped field are
// ignored: seeding never invents a sample. Set(zset.New()) resets.
func (o *Stamp) Set(v zset.ZSet) {
	o.state = seedStampState(o.keyExpr, o.fields, v)
}

// Apply consumes a full level and emits the stamped level.
func (o *Stamp) Apply(ctx *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	now := execNow(ctx)
	out := zset.New()
	next := map[string]*stampEntry{}
	var iterErr error

	inputs[0].Iter(func(elem datamodel.Document, w zset.Weight) bool {
		id, key, err := evaluateGroupKey(elem, o.keyExpr, now)
		if err != nil {
			iterErr = fmt.Errorf("stamp: %w", err)
			return false
		}
		entry := o.state[id] // held from the previous step
		if entry == nil {
			entry = next[id] // sampled earlier this step for the same new key
		}
		if entry == nil {
			values, err := sampleStamp(o.fields, elem, now)
			if err != nil {
				iterErr = err
				return false
			}
			entry = &stampEntry{key: key, values: values}
		}
		next[id] = entry

		stamped, err := applyStamp(o.fields, elem, entry)
		if err != nil {
			iterErr = err
			return false
		}
		out.Insert(stamped, w)
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}
	o.state = next
	if o.logger.V(2).Enabled() {
		o.logger.V(2).Info("operator", "op", o.String(), "result", out.String())
	}
	return out, nil
}

// StampIncremental is the delta form: a stateful delta processor whose
// state is the integral of its own output projected to (key, held values,
// row weights). A row's stamp is decided by looking its key up in the
// state as it stood at the end of the previous step: a held key reuses its
// values, an unknown key samples once for every edge of that key this
// step, of either sign. The step's weights are folded into the state only
// after the whole input has been processed, so same-step retract/insert
// pairs on one key keep their stamp and the iteration order of the input
// is irrelevant. This is the lifted-function-over-delayed-integral shape
// of DistinctH, reading the integral of its own output.
type StampIncremental struct {
	baseOp
	keyExpr expression.Expression
	fields  []stampField
	state   map[string]*stampEntry
}

// NewStampIncremental creates the delta form directly.
func NewStampIncremental(keyExpr expression.Expression, fields map[string]expression.Expression, opts ...Option) *StampIncremental {
	return &StampIncremental{
		baseOp:  newBaseOp("stamp_incremental", opts),
		keyExpr: keyExpr,
		fields:  sortStampFields(fields),
		state:   map[string]*stampEntry{},
	}
}

func (o *StampIncremental) Kind() Kind           { return KindStampIncremental }
func (o *StampIncremental) String() string       { return "StampIncremental" }
func (o *StampIncremental) Arity() int           { return 1 }
func (o *StampIncremental) Linearity() Linearity { return Primitive }

// Set seeds the state from a level of already-stamped rows: the held
// values and the key weights. Rows missing a stamped field are ignored.
func (o *StampIncremental) Set(v zset.ZSet) {
	o.state = seedStampState(o.keyExpr, o.fields, v)
}

// Apply implements Operator.
func (o *StampIncremental) Apply(ctx *ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	now := execNow(ctx)
	out := zset.New()

	// Samples decided this step for keys absent from the previous state,
	// populated on first sight so every edge of the key gets the same one.
	fresh := map[string]*stampEntry{}
	weights := map[string]map[string]zset.Weight{} // key id -> row hash -> weight
	var iterErr error

	inputs[0].Iter(func(elem datamodel.Document, w zset.Weight) bool {
		id, key, err := evaluateGroupKey(elem, o.keyExpr, now)
		if err != nil {
			iterErr = fmt.Errorf("stamp: %w", err)
			return false
		}
		entry := o.state[id] // previous-step state, never mutated in this loop
		if entry == nil {
			entry = fresh[id]
		}
		if entry == nil {
			values, err := sampleStamp(o.fields, elem, now)
			if err != nil {
				iterErr = err
				return false
			}
			entry = &stampEntry{key: key, values: values, weights: map[string]zset.Weight{}}
			fresh[id] = entry
		}
		stamped, err := applyStamp(o.fields, elem, entry)
		if err != nil {
			iterErr = err
			return false
		}
		if weights[id] == nil {
			weights[id] = map[string]zset.Weight{}
		}
		weights[id][stamped.Hash()] += w
		out.Insert(stamped, w)
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}

	// Integrate: fold the step's row weights into the state, keeping debt
	// (negative weights) and dropping keys none of whose rows remain.
	for id, rows := range weights {
		entry := o.state[id]
		if entry == nil {
			entry = fresh[id]
			o.state[id] = entry
		}
		for h, w := range rows {
			entry.weights[h] += w
			if entry.weights[h] == 0 {
				delete(entry.weights, h)
			}
		}
		if len(entry.weights) == 0 {
			delete(o.state, id)
		}
	}

	if o.logger.V(2).Enabled() {
		o.logger.V(2).Info("operator", "op", o.String(), "result", out.String(), "live_keys", len(o.state))
	}
	return out, nil
}

// sampleStamp evaluates the value expressions for a key seen for the
// first time. A field the document already carries is respected.
func sampleStamp(fields []stampField, doc datamodel.Document, now string) (map[string]any, error) {
	values := make(map[string]any, len(fields))
	for _, f := range fields {
		if v, err := doc.GetField(f.path); err == nil && v != nil {
			values[f.path] = v
			continue
		}
		if f.expr == nil {
			return nil, fmt.Errorf("stamp: field %s has no value and no expression", f.path)
		}
		v, err := f.expr.Evaluate(expression.NewContext(doc).WithNow(now).WithSubject(doc))
		if err != nil && !errors.Is(err, datamodel.ErrFieldNotFound) {
			return nil, fmt.Errorf("stamp: value expression for %s: %w", f.path, err)
		}
		values[f.path] = v
	}
	return values, nil
}

// applyStamp returns a copy of doc with the held values written in.
func applyStamp(fields []stampField, doc datamodel.Document, entry *stampEntry) (datamodel.Document, error) {
	stamped := doc.Copy()
	for _, f := range fields {
		if err := stamped.SetField(f.path, entry.values[f.path]); err != nil {
			return nil, fmt.Errorf("stamp: set %s: %w", f.path, err)
		}
	}
	return stamped, nil
}

// seedStampState builds the held map from a level of stamped rows, the
// operator's own output integral as far as the caller knows it.
func seedStampState(keyExpr expression.Expression, fields []stampField, v zset.ZSet) map[string]*stampEntry {
	state := map[string]*stampEntry{}
	v.Iter(func(elem datamodel.Document, w zset.Weight) bool {
		id, key, err := evaluateGroupKey(elem, keyExpr, "")
		if err != nil {
			return true
		}
		values := make(map[string]any, len(fields))
		for _, f := range fields {
			fv, err := elem.GetField(f.path)
			if err != nil {
				return true
			}
			values[f.path] = fv
		}
		e := state[id]
		if e == nil {
			e = &stampEntry{key: key, values: values, weights: map[string]zset.Weight{}}
			state[id] = e
		}
		e.weights[elem.Hash()] += w
		return true
	})
	for id, e := range state {
		for h, w := range e.weights {
			if w == 0 {
				delete(e.weights, h)
			}
		}
		if len(e.weights) == 0 {
			delete(state, id)
		}
	}
	return state
}

// JSON wire format: {"type": "stamp", "keyExpr": ..., "fields": {"$.path":
// valueExpr | null}}, and the same with type "stamp_incremental".

func marshalStamp(opType string, keyExpr expression.Expression, fields []stampField) ([]byte, error) {
	keyJSON, err := json.Marshal(keyExpr)
	if err != nil {
		return nil, fmt.Errorf("marshal %s keyExpr: %w", opType, err)
	}
	raw := make(map[string]json.RawMessage, len(fields))
	for _, f := range fields {
		if f.expr == nil {
			raw[f.path] = json.RawMessage("null")
			continue
		}
		b, err := json.Marshal(f.expr)
		if err != nil {
			return nil, fmt.Errorf("marshal %s field %s: %w", opType, f.path, err)
		}
		raw[f.path] = b
	}
	return json.Marshal(jsonOp{Type: opType, KeyExpr: keyJSON, Fields: raw})
}

func unmarshalStamp(opType string, p jsonOp) (expression.Expression, map[string]expression.Expression, error) {
	if len(p.KeyExpr) == 0 || string(p.KeyExpr) == "null" {
		return nil, nil, fmt.Errorf("%s: keyExpr is required", opType)
	}
	if len(p.Fields) == 0 {
		return nil, nil, fmt.Errorf("%s: fields is required", opType)
	}
	keyExpr, err := dbspexpr.Compile(p.KeyExpr)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: compile keyExpr: %w", opType, err)
	}
	fields := make(map[string]expression.Expression, len(p.Fields))
	for path, raw := range p.Fields {
		if string(raw) == "null" {
			fields[path] = nil
			continue
		}
		e, err := dbspexpr.Compile(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: compile field %s: %w", opType, path, err)
		}
		fields[path] = e
	}
	return keyExpr, fields, nil
}

// MarshalJSON implements json.Marshaler.
func (o *Stamp) MarshalJSON() ([]byte, error) { return marshalStamp("stamp", o.keyExpr, o.fields) }

// UnmarshalJSON implements json.Unmarshaler.
func (o *Stamp) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	keyExpr, fields, err := unmarshalStamp("stamp", p)
	if err != nil {
		return err
	}
	*o = *NewStamp(keyExpr, fields)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (o *StampIncremental) MarshalJSON() ([]byte, error) {
	return marshalStamp("stamp_incremental", o.keyExpr, o.fields)
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *StampIncremental) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	keyExpr, fields, err := unmarshalStamp("stamp_incremental", p)
	if err != nil {
		return err
	}
	*o = *NewStampIncremental(keyExpr, fields)
	return nil
}
