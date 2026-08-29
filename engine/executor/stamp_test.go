package executor

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/internal/logger"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
)

// A status condition keyed on (obj, type, status), stamped with the round
// clock: a status flip restamps, a reason edit holds.

func stampCondition(obj, status, reason string) datamodel.Document {
	return unstructured.New(map[string]any{
		"obj": obj, "type": "Ready", "status": status, "reason": reason,
	})
}

func stampOperator() *operator.Stamp {
	key, err := dbspexpr.CompileString(`["$.obj", "$.type", "$.status"]`)
	Expect(err).NotTo(HaveOccurred())
	now, err := dbspexpr.CompileString(`{"@now": null}`)
	Expect(err).NotTo(HaveOccurred())
	return operator.NewStamp(key, map[string]expression.Expression{"$.lastTransitionTime": now})
}

func stampCircuit() *circuit.Circuit {
	c := circuit.New("stamp-test")
	c.AddNode(circuit.Input("in"))
	c.AddNode(circuit.Op("stamp", stampOperator()))
	c.AddNode(circuit.Output("out"))
	c.AddEdge(circuit.NewEdge("in", "stamp", 0))
	c.AddEdge(circuit.NewEdge("stamp", "out", 0))
	return c
}

// randomConditionDeltas draws delta sequences over a small universe with
// sign-boundary crossings.
func randomConditionDeltas(seed int64, steps int) []zset.ZSet {
	objs := []string{"a", "b", "c"}
	statuses := []string{"True", "False"}
	reasons := []string{"r1", "r2", "r3"}
	weights := []zset.Weight{-1, 1, -1, 1, -2, 2}
	rng := rand.New(rand.NewSource(seed))
	out := make([]zset.ZSet, 0, steps)
	for step := 0; step < steps; step++ {
		d := zset.New()
		for i, n := 0, 1+rng.Intn(4); i < n; i++ {
			doc := stampCondition(objs[rng.Intn(len(objs))], statuses[rng.Intn(len(statuses))], reasons[rng.Intn(len(reasons))])
			d.Insert(doc, weights[rng.Intn(len(weights))])
		}
		out = append(out, d)
	}
	return out
}

// A clock that advances one minute per round, shared by every executor of a
// test so the snapshot and incremental circuits see the same τ per step.
type testClock struct{ t time.Time }

func (c *testClock) now() time.Time { return c.t }
func (c *testClock) tick()          { c.t = c.t.Add(time.Minute) }

var _ = Describe("Stamp", func() {
	It("snapshot circuit vs incremental circuit, clock advancing", func() {
		normal := stampCircuit()
		incr, err := transform.NewIncrementalizer().Transform(normal)
		Expect(err).NotTo(HaveOccurred())
		Expect(incr.Node("stamp^Δ").Kind()).To(Equal(operator.KindStampIncremental))

		clock := &testClock{t: time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)}
		normalExec, err := New(normal, logger.NewZapLogger(logLevel))
		Expect(err).NotTo(HaveOccurred())
		normalExec.SetClock(clock.now)
		incrExec, err := New(incr, logger.NewZapLogger(logLevel))
		Expect(err).NotTo(HaveOccurred())
		incrExec.SetClock(clock.now)

		// The snapshot executor is NOT reset between rounds: Stamp is
		// stateful by design, the held stamps are part of its state.
		level := zset.New()
		var prevNormal zset.ZSet
		for step, d := range randomConditionDeltas(20260829, 200) {
			level = level.Add(d)
			normalOut, err := normalExec.Execute(map[string]zset.ZSet{"in": level})
			Expect(err).NotTo(HaveOccurred(), "step %d", step)
			incrOut, err := incrExec.Execute(map[string]zset.ZSet{"in": d})
			Expect(err).NotTo(HaveOccurred(), "step %d", step)

			expected := normalOut["out"]
			if step > 0 {
				expected = normalOut["out"].Subtract(prevNormal)
			}
			Expect(incrOut["out"].Equal(expected)).To(BeTrue(),
				"step %d: incremental %s != D(snapshot) %s", step, incrOut["out"].String(), expected.String())
			prevNormal = normalOut["out"]
			clock.tick()
		}
	})

	It("emits nothing while the level holds and the clock moves", func() {
		normal := stampCircuit()
		incr, err := transform.NewIncrementalizer().Transform(normal)
		Expect(err).NotTo(HaveOccurred())

		clock := &testClock{t: time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)}
		normalExec, _ := New(normal, logger.NewZapLogger(logLevel))
		normalExec.SetClock(clock.now)
		incrExec, _ := New(incr, logger.NewZapLogger(logLevel))
		incrExec.SetClock(clock.now)

		level := zset.New().WithElems(zset.Elem{Document: stampCondition("x", "True", "Ok"), Weight: 1})
		first, err := normalExec.Execute(map[string]zset.ZSet{"in": level})
		Expect(err).NotTo(HaveOccurred())
		firstIncr, err := incrExec.Execute(map[string]zset.ZSet{"in": level})
		Expect(err).NotTo(HaveOccurred())
		Expect(firstIncr["out"].Equal(first["out"])).To(BeTrue())
		stamp, err := first["out"].Entries()[0].Document.GetField("$.lastTransitionTime")
		Expect(err).NotTo(HaveOccurred())
		Expect(stamp).To(Equal("2026-01-01T10:00:00Z"))

		for step := 0; step < 3; step++ {
			clock.tick()
			again, err := normalExec.Execute(map[string]zset.ZSet{"in": level})
			Expect(err).NotTo(HaveOccurred())
			Expect(again["out"].Equal(first["out"])).To(BeTrue(), "snapshot restamped at step %d", step)
			delta, err := incrExec.Execute(map[string]zset.ZSet{"in": zset.New()})
			Expect(err).NotTo(HaveOccurred())
			Expect(delta["out"].IsZero()).To(BeTrue(), "incremental emitted at step %d: %s", step, delta["out"].String())
		}
	})

	It("is the circuit StampF -> ∫ -> z⁻¹ -> StampF over a pure stamping function", func() {
		// The explicit construction of section 3 of the design: f(E, Mprev,
		// τ) is a pure two-input operator, Mprev the delayed integral of its
		// own output. Step-for-step equal to the fused StampIncremental.
		f := &stampF{op: stampOperator()}
		explicit := circuit.New("stamp-explicit")
		explicit.AddNode(circuit.Input("in"))
		explicit.AddNode(circuit.Op("f", f))
		explicit.AddNode(circuit.Delay("delay"))
		explicit.AddNode(circuit.Integrate("int"))
		explicit.AddNode(circuit.Output("out"))
		explicit.AddEdge(circuit.NewEdge("in", "f", 0))
		explicit.AddEdge(circuit.NewEdge("f", "out", 0))
		explicit.AddEdge(circuit.NewEdge("f", "delay", 0))
		explicit.AddEdge(circuit.NewEdge("delay", "int", 0))
		explicit.AddEdge(circuit.NewEdge("int", "f", 1))
		Expect(explicit.Validate()).To(BeEmpty())

		fused := circuit.New("stamp-fused")
		fused.AddNode(circuit.Input("in"))
		fused.AddNode(circuit.Op("stamp", stampOperator().Incremental()))
		fused.AddNode(circuit.Output("out"))
		fused.AddEdge(circuit.NewEdge("in", "stamp", 0))
		fused.AddEdge(circuit.NewEdge("stamp", "out", 0))

		clock := &testClock{t: time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)}
		explicitExec, err := New(explicit, logger.NewZapLogger(logLevel))
		Expect(err).NotTo(HaveOccurred())
		explicitExec.SetClock(clock.now)
		fusedExec, err := New(fused, logger.NewZapLogger(logLevel))
		Expect(err).NotTo(HaveOccurred())
		fusedExec.SetClock(clock.now)

		for step, d := range randomConditionDeltas(1337, 200) {
			e, err := explicitExec.Execute(map[string]zset.ZSet{"in": d})
			Expect(err).NotTo(HaveOccurred(), "step %d", step)
			g, err := fusedExec.Execute(map[string]zset.ZSet{"in": d})
			Expect(err).NotTo(HaveOccurred(), "step %d", step)
			Expect(g["out"].Equal(e["out"])).To(BeTrue(),
				"step %d: fused %s != explicit %s", step, g["out"].String(), e["out"].String())
			clock.tick()
		}
	})
})

// stampF is the pure stamping function f(E, Mprev, τ) as a two-input
// operator: port 0 the delta, port 1 the integral of its own output as it
// stood at the end of the previous step. Stateless; the held stamps are
// read off Mprev.
type stampF struct {
	op *operator.Stamp
}

func (f *stampF) Kind() operator.Kind           { return operator.KindNoOp }
func (f *stampF) String() string                { return "StampF" }
func (f *stampF) Arity() int                    { return 2 }
func (f *stampF) Linearity() operator.Linearity { return operator.Primitive }
func (f *stampF) Set(_ zset.ZSet)               {}
func (f *stampF) MarshalJSON() ([]byte, error)  { return json.Marshal(map[string]any{"type": "stampF"}) }
func (f *stampF) UnmarshalJSON(_ []byte) error  { return nil }

func (f *stampF) Apply(ctx *operator.ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	keyExpr := f.op.KeyExpr()
	fields := f.op.Fields()
	keyOf := func(doc datamodel.Document) (string, error) {
		v, err := keyExpr.Evaluate(expression.NewContext(doc).WithSubject(doc))
		if err != nil {
			return "", err
		}
		b, err := json.Marshal(v)
		return string(b), err
	}

	// Held stamps: one per key present in Mprev with non-zero weight.
	held := map[string]map[string]any{}
	var iterErr error
	inputs[1].Iter(func(doc datamodel.Document, _ zset.Weight) bool {
		id, err := keyOf(doc)
		if err != nil {
			iterErr = err
			return false
		}
		if _, ok := held[id]; ok {
			return true
		}
		values := map[string]any{}
		for path := range fields {
			v, err := doc.GetField(path)
			if err != nil {
				iterErr = fmt.Errorf("Mprev row without stamp: %w", err)
				return false
			}
			values[path] = v
		}
		held[id] = values
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}

	// Stamp E: held keys reuse, new keys sample τ once.
	out := zset.New()
	fresh := map[string]map[string]any{}
	inputs[0].Iter(func(doc datamodel.Document, w zset.Weight) bool {
		id, err := keyOf(doc)
		if err != nil {
			iterErr = err
			return false
		}
		values, ok := held[id]
		if !ok {
			values, ok = fresh[id]
		}
		if !ok {
			values = map[string]any{}
			for path, e := range fields {
				v, err := e.Evaluate(expression.NewContext(doc).WithNow(ctx.Now).WithSubject(doc))
				if err != nil {
					iterErr = err
					return false
				}
				values[path] = v
			}
			fresh[id] = values
		}
		stamped := doc.Copy()
		for path, v := range values {
			if err := stamped.SetField(path, v); err != nil {
				iterErr = err
				return false
			}
		}
		out.Insert(stamped, w)
		return true
	})
	if iterErr != nil {
		return zset.New(), iterErr
	}
	return out, nil
}
