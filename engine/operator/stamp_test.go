package operator

import (
	"fmt"
	"math/rand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/zset"
)

// The stamp tests model a status condition keyed on (obj, type, status):
// a status flip restamps, a reason edit holds.

func condition(obj, status, reason string) datamodel.Document {
	return unstructured.New(map[string]any{
		"obj": obj, "type": "Ready", "status": status, "reason": reason,
	})
}

func stampedCondition(obj, status, reason, stamp string) datamodel.Document {
	return unstructured.New(map[string]any{
		"obj": obj, "type": "Ready", "status": status, "reason": reason, "lastTransitionTime": stamp,
	})
}

func conditionKey() expression.Expression {
	e, err := dbspexpr.CompileString(`["$.obj", "$.type", "$.status"]`)
	Expect(err).NotTo(HaveOccurred())
	return e
}

func nowField() map[string]expression.Expression {
	e, err := dbspexpr.CompileString(`{"@now": null}`)
	Expect(err).NotTo(HaveOccurred())
	return map[string]expression.Expression{"$.lastTransitionTime": e}
}

func at(clock string) *ExecContext { return &ExecContext{Now: clock} }

func delta(es ...zset.Elem) zset.ZSet { return zset.New().WithElems(es...) }

var _ = Describe("Stamp", func() {
	Describe("StampIncremental (delta form)", func() {
		It("follows the worked trace: hold across edits, restamp on flip, retract with the held stamp", func() {
			op := NewStampIncremental(conditionKey(), nowField())

			// Step 1, 10:00: the condition appears.
			out, err := op.Apply(at("10:00"), delta(zset.Elem{Document: condition("x", "False", "Init"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "False", "Init", "10:00"), Weight: 1}))).To(BeTrue(), out.String())

			// Step 2, 10:01: nothing happens; the clock moves, no work.
			out, err = op.Apply(at("10:01"), zset.New())
			Expect(err).NotTo(HaveOccurred())
			Expect(out.IsZero()).To(BeTrue())

			// Step 3, 10:02: the reason changes; the key is held, both edges
			// carry 10:00, the retraction cancels the step-1 insertion.
			out, err = op.Apply(at("10:02"), delta(
				zset.Elem{Document: condition("x", "False", "Init"), Weight: -1},
				zset.Elem{Document: condition("x", "False", "Waiting"), Weight: 1},
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(
				zset.Elem{Document: stampedCondition("x", "False", "Init", "10:00"), Weight: -1},
				zset.Elem{Document: stampedCondition("x", "False", "Waiting", "10:00"), Weight: 1},
			))).To(BeTrue(), out.String())

			// Step 4, 10:05: a genuine transition; the old key drains, the
			// new one samples.
			out, err = op.Apply(at("10:05"), delta(
				zset.Elem{Document: condition("x", "False", "Waiting"), Weight: -1},
				zset.Elem{Document: condition("x", "True", "Ok"), Weight: 1},
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(
				zset.Elem{Document: stampedCondition("x", "False", "Waiting", "10:00"), Weight: -1},
				zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:05"), Weight: 1},
			))).To(BeTrue(), out.String())
			Expect(op.state).To(HaveLen(1))

			// Step 5, 10:09: retraction carries the held 10:05, state empties.
			out, err = op.Apply(at("10:09"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: -1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:05"), Weight: -1}))).To(BeTrue(), out.String())
			Expect(op.state).To(BeEmpty())
		})

		It("is independent of the input iteration order", func() {
			forward := NewStampIncremental(conditionKey(), nowField())
			reverse := NewStampIncremental(conditionKey(), nowField())
			seed := delta(zset.Elem{Document: condition("x", "False", "Init"), Weight: 1})
			_, err := forward.Apply(at("10:00"), seed)
			Expect(err).NotTo(HaveOccurred())
			_, err = reverse.Apply(at("10:00"), seed)
			Expect(err).NotTo(HaveOccurred())

			// The same edit, built in both orders, on a step where a
			// second, new key appears twice as well.
			a := zset.Elem{Document: condition("x", "False", "Init"), Weight: -1}
			b := zset.Elem{Document: condition("x", "False", "Waiting"), Weight: 1}
			c := zset.Elem{Document: condition("y", "True", "Ok"), Weight: 1}
			d := zset.Elem{Document: condition("y", "True", "Fine"), Weight: 1}
			o1, err := forward.Apply(at("10:02"), delta(a, b, c, d))
			Expect(err).NotTo(HaveOccurred())
			o2, err := reverse.Apply(at("10:02"), delta(d, c, b, a))
			Expect(err).NotTo(HaveOccurred())
			Expect(o1.Equal(o2)).To(BeTrue())
			Expect(o1.Equal(delta(
				zset.Elem{Document: stampedCondition("x", "False", "Init", "10:00"), Weight: -1},
				zset.Elem{Document: stampedCondition("x", "False", "Waiting", "10:00"), Weight: 1},
				zset.Elem{Document: stampedCondition("y", "True", "Ok", "10:02"), Weight: 1},
				zset.Elem{Document: stampedCondition("y", "True", "Fine", "10:02"), Weight: 1},
			))).To(BeTrue(), o1.String())
		})

		It("keeps debt: a retraction before its insertion holds the sample until the key nets to zero", func() {
			op := NewStampIncremental(conditionKey(), nowField())
			out, err := op.Apply(at("10:00"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: -1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:00"), Weight: -1}))).To(BeTrue())
			Expect(op.state).To(HaveLen(1))

			// The matching insertion later cancels exactly and drains the key.
			out, err = op.Apply(at("10:03"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:00"), Weight: 1}))).To(BeTrue())
			Expect(op.state).To(BeEmpty())

			// Afterwards the key is unknown again and samples afresh.
			out, err = op.Apply(at("10:07"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:07"), Weight: 1}))).To(BeTrue())
		})

		It("holds through multiplicity above one", func() {
			op := NewStampIncremental(conditionKey(), nowField())
			_, err := op.Apply(at("10:00"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: 2}))
			Expect(err).NotTo(HaveOccurred())

			out, err := op.Apply(at("10:01"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: -1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:00"), Weight: -1}))).To(BeTrue())
			Expect(op.state).To(HaveLen(1))

			out, err = op.Apply(at("10:02"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: -1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:00"), Weight: -1}))).To(BeTrue())
			Expect(op.state).To(BeEmpty())
		})

		It("holds a key that disappears and reappears within one step", func() {
			op := NewStampIncremental(conditionKey(), nowField())
			_, err := op.Apply(at("10:00"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())

			// The -old +new pair sums to zero on the key: no drain.
			out, err := op.Apply(at("10:04"), delta(
				zset.Elem{Document: condition("x", "True", "Ok"), Weight: -1},
				zset.Elem{Document: condition("x", "True", "Fine"), Weight: 1},
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(
				zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:00"), Weight: -1},
				zset.Elem{Document: stampedCondition("x", "True", "Fine", "10:00"), Weight: 1},
			))).To(BeTrue(), out.String())
			Expect(op.state).To(HaveLen(1))
		})

		It("respects a value the document already carries", func() {
			op := NewStampIncremental(conditionKey(), nowField())
			out, err := op.Apply(at("10:00"), delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "09:00"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "09:00"), Weight: 1}))).To(BeTrue())

			// Held for the key's lifetime, even against a later edit that
			// carries a different value.
			out, err = op.Apply(at("10:05"), delta(
				zset.Elem{Document: stampedCondition("x", "True", "Ok", "09:00"), Weight: -1},
				zset.Elem{Document: stampedCondition("x", "True", "Fine", "10:05"), Weight: 1},
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(
				zset.Elem{Document: stampedCondition("x", "True", "Ok", "09:00"), Weight: -1},
				zset.Elem{Document: stampedCondition("x", "True", "Fine", "09:00"), Weight: 1},
			))).To(BeTrue(), out.String())
		})

		It("fails on a field with neither a value nor an expression", func() {
			op := NewStampIncremental(conditionKey(), map[string]expression.Expression{"$.lastTransitionTime": nil})
			_, err := op.Apply(at("10:00"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: 1}))
			Expect(err).To(MatchError(ContainSubstring("has no value and no expression")))

			// With the value present the nil expression holds it.
			out, err := op.Apply(at("10:00"), delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "09:00"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(1))
		})

		It("seeds from a level of stamped rows and ignores unstamped ones", func() {
			op := NewStampIncremental(conditionKey(), nowField())
			op.Set(delta(
				zset.Elem{Document: stampedCondition("x", "True", "Ok", "08:00"), Weight: 1},
				zset.Elem{Document: condition("y", "True", "Ok"), Weight: 1},
			))
			Expect(op.state).To(HaveLen(1))

			out, err := op.Apply(at("10:00"), delta(
				zset.Elem{Document: condition("x", "True", "Ok"), Weight: -1},
				zset.Elem{Document: condition("x", "True", "Fine"), Weight: 1},
				zset.Elem{Document: condition("y", "True", "Fine"), Weight: 1},
			))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(
				zset.Elem{Document: stampedCondition("x", "True", "Ok", "08:00"), Weight: -1},
				zset.Elem{Document: stampedCondition("x", "True", "Fine", "08:00"), Weight: 1},
				zset.Elem{Document: stampedCondition("y", "True", "Fine", "10:00"), Weight: 1},
			))).To(BeTrue(), out.String())

			op.Set(zset.New())
			Expect(op.state).To(BeEmpty())
		})

		It("round-trips through JSON", func() {
			op := NewStampIncremental(conditionKey(), nowField())
			b, err := op.MarshalJSON()
			Expect(err).NotTo(HaveOccurred())
			again, err := UnmarshalOperator(b)
			Expect(err).NotTo(HaveOccurred())
			Expect(again.Kind()).To(Equal(KindStampIncremental))
			out, err := again.Apply(at("10:00"), delta(zset.Elem{Document: condition("x", "True", "Ok"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:00"), Weight: 1}))).To(BeTrue())

			snap := NewStamp(conditionKey(), nowField())
			b, err = snap.MarshalJSON()
			Expect(err).NotTo(HaveOccurred())
			again, err = UnmarshalOperator(b)
			Expect(err).NotTo(HaveOccurred())
			Expect(again.Kind()).To(Equal(KindStamp))
			Expect(again.Linearity()).To(Equal(NonLinear))
		})
	})

	Describe("Stamp (snapshot form)", func() {
		It("holds across steps and forgets keys absent from the level", func() {
			op := NewStamp(conditionKey(), nowField())
			level := delta

			out, err := op.Apply(at("10:00"), level(zset.Elem{Document: condition("x", "False", "Init"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(level(zset.Elem{Document: stampedCondition("x", "False", "Init", "10:00"), Weight: 1}))).To(BeTrue())

			// Same level, later clock: identical output (the regression pin
			// against a stateless snapshot form).
			out, err = op.Apply(at("10:01"), level(zset.Elem{Document: condition("x", "False", "Init"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(level(zset.Elem{Document: stampedCondition("x", "False", "Init", "10:00"), Weight: 1}))).To(BeTrue())

			// Reason edit: held.
			out, err = op.Apply(at("10:02"), level(zset.Elem{Document: condition("x", "False", "Waiting"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(level(zset.Elem{Document: stampedCondition("x", "False", "Waiting", "10:00"), Weight: 1}))).To(BeTrue())

			// Status flip: restamped.
			out, err = op.Apply(at("10:05"), level(zset.Elem{Document: condition("x", "True", "Ok"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(level(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:05"), Weight: 1}))).To(BeTrue())

			// Gone, then back: sampled afresh.
			out, err = op.Apply(at("10:09"), level())
			Expect(err).NotTo(HaveOccurred())
			Expect(out.IsZero()).To(BeTrue())
			out, err = op.Apply(at("10:12"), level(zset.Elem{Document: condition("x", "True", "Ok"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(level(zset.Elem{Document: stampedCondition("x", "True", "Ok", "10:12"), Weight: 1}))).To(BeTrue())
		})

		It("seeds from a level via Set", func() {
			op := NewStamp(conditionKey(), nowField())
			op.Set(delta(zset.Elem{Document: stampedCondition("x", "True", "Ok", "08:00"), Weight: 1}))
			out, err := op.Apply(at("10:00"), delta(zset.Elem{Document: condition("x", "True", "Fine"), Weight: 1}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Equal(delta(zset.Elem{Document: stampedCondition("x", "True", "Fine", "08:00"), Weight: 1}))).To(BeTrue())
		})
	})

	Describe("Pair equivalence: Stamp = ∫ ∘ StampIncremental ∘ D", func() {
		It("agrees step for step over random delta sequences with the clock advancing", func() {
			objs := []string{"a", "b", "c"}
			statuses := []string{"True", "False"}
			reasons := []string{"r1", "r2", "r3"}
			weights := []zset.Weight{-1, 1, -1, 1, -2, 2}
			rng := rand.New(rand.NewSource(20260829))

			snap := NewStamp(conditionKey(), nowField())
			incr := NewStampIncremental(conditionKey(), nowField())
			inputLevel := zset.New()
			outputLevel := zset.New()

			for step := 0; step < 300; step++ {
				d := zset.New()
				for i, n := 0, 1+rng.Intn(4); i < n; i++ {
					doc := condition(objs[rng.Intn(len(objs))], statuses[rng.Intn(len(statuses))], reasons[rng.Intn(len(reasons))])
					d.Insert(doc, weights[rng.Intn(len(weights))])
				}
				clock := fmt.Sprintf("t%03d", step)

				inputLevel = inputLevel.Add(d)
				snapOut, err := snap.Apply(at(clock), inputLevel)
				Expect(err).NotTo(HaveOccurred(), "step %d", step)

				incrOut, err := incr.Apply(at(clock), d)
				Expect(err).NotTo(HaveOccurred(), "step %d", step)
				outputLevel = outputLevel.Add(incrOut)

				Expect(outputLevel.Equal(snapOut)).To(BeTrue(),
					"step %d: ∫(delta out) %s != snapshot out %s", step, outputLevel.String(), snapOut.String())
			}
		})
	})
})
