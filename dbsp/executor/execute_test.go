package executor

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.uber.org/zap/zapcore"

	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/operator"
	"github.com/l7mp/dbsp/dbsp/transform"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/expression"
	"github.com/l7mp/dbsp/internal/logger"
	"github.com/l7mp/dbsp/internal/testutils"
)

const logLevel = zapcore.ErrorLevel

func TestExecute(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Execute Suite")
}

var _ = Describe("Executor", func() {
	Describe("Basic execution", func() {
		It("executes a simple linear circuit", func() {
			// in -> select (value > 5) -> out.
			c := circuit.New("simple")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("sel", operator.NewSelect("σ",
				expression.Func(func(ctx *expression.EvalContext) (any, error) {
					e := ctx.Document().(testutils.Record)
					return e.Value > 5, nil
				}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "out", 0))

			exec, err := New(c, logger.NewZapLogger(logLevel))
			Expect(err).NotTo(HaveOccurred())

			input := zset.New()
			record1 := testutils.Record{ID: "a", Value: 3}
			record2 := testutils.Record{ID: "b", Value: 7}
			input.Insert(record1, 1)
			input.Insert(record2, 1)

			output, err := exec.Execute(map[string]zset.ZSet{"in": input})
			Expect(err).NotTo(HaveOccurred())
			Expect(output["out"].Size()).To(Equal(1))
			Expect(output["out"].Lookup(record2.Hash())).To(Equal(zset.Weight(1)))
		})

		It("handles missing inputs as empty", func() {
			c := circuit.New("missing-input")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "out", 0))

			exec, err := New(c, logger.NewZapLogger(logLevel))
			Expect(err).NotTo(HaveOccurred())

			output, err := exec.Execute(map[string]zset.ZSet{})
			Expect(err).NotTo(HaveOccurred())
			Expect(output["out"].IsZero()).To(BeTrue())
		})
	})

	Describe("Stateful nodes", func() {
		It("delay outputs previous value", func() {
			// in -> delay -> out.
			c := circuit.New("delay-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Delay("z-1"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "z-1", 0))
			c.AddEdge(circuit.NewEdge("z-1", "out", 0))

			exec, err := New(c, logger.NewZapLogger(logLevel))
			Expect(err).NotTo(HaveOccurred())

			recordA := testutils.Record{ID: "a", Value: 1}
			recordB := testutils.Record{ID: "b", Value: 2}
			recordC := testutils.Record{ID: "c", Value: 3}

			// Step 1: Input A, output empty (first step).
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": zset.New().WithElems(zset.Elem{Document: recordA, Weight: 1})})
			Expect(r1["out"].IsZero()).To(BeTrue())

			// Step 2: Input B, output A (previous).
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": zset.New().WithElems(zset.Elem{Document: recordB, Weight: 1})})
			Expect(r2["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(1)))

			// Step 3: Input C, output B.
			r3, _ := exec.Execute(map[string]zset.ZSet{"in": zset.New().WithElems(zset.Elem{Document: recordC, Weight: 1})})
			Expect(r3["out"].Lookup(recordB.Hash())).To(Equal(zset.Weight(1)))
		})

		It("integrate accumulates values", func() {
			// in -> integrate -> out.
			c := circuit.New("integrate-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "out", 0))

			exec, err := New(c, logger.NewZapLogger(logLevel))
			Expect(err).NotTo(HaveOccurred())

			recordA := testutils.Record{ID: "a", Value: 1}
			recordB := testutils.Record{ID: "b", Value: 2}

			// Step 1: Input A.
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": zset.New().WithElems(zset.Elem{Document: recordA, Weight: 1})})
			Expect(r1["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(1)))

			// Step 2: Input A again (should accumulate).
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": zset.New().WithElems(zset.Elem{Document: recordA, Weight: 1})})
			Expect(r2["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(2)))

			// Step 3: Input B.
			r3, _ := exec.Execute(map[string]zset.ZSet{"in": zset.New().WithElems(zset.Elem{Document: recordB, Weight: 1})})
			Expect(r3["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(2)))
			Expect(r3["out"].Lookup(recordB.Hash())).To(Equal(zset.Weight(1)))
		})

		It("differentiate computes difference", func() {
			// in -> differentiate -> out.
			c := circuit.New("diff-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "out", 0))

			exec, err := New(c, logger.NewZapLogger(logLevel))
			Expect(err).NotTo(HaveOccurred())

			recordA := testutils.Record{ID: "a", Value: 1}

			// Step 1: Input {A:1}, output {A:1} (first step = input).
			input1 := zset.New().WithElems(zset.Elem{Document: recordA, Weight: 1})
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": input1})
			Expect(r1["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(1)))

			// Step 2: Input {A:2}, output {A:1} (2-1=1).
			input2 := zset.New().WithElems(zset.Elem{Document: recordA, Weight: 2})
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": input2})
			Expect(r2["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(1)))

			// Step 3: Input {A:1}, output {A:-1} (1-2=-1).
			input3 := zset.New().WithElems(zset.Elem{Document: recordA, Weight: 1})
			r3, _ := exec.Execute(map[string]zset.ZSet{"in": input3})
			Expect(r3["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(-1)))
		})

		It("delta0 fires only once", func() {
			// in -> delta0 -> out.
			c := circuit.New("delta0-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Delta0("d0"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "d0", 0))
			c.AddEdge(circuit.NewEdge("d0", "out", 0))

			exec, err := New(c, logger.NewZapLogger(logLevel))
			Expect(err).NotTo(HaveOccurred())

			recordA := testutils.Record{ID: "a", Value: 1}
			input := zset.New().WithElems(zset.Elem{Document: recordA, Weight: 1})

			// Step 1: Input A, output A.
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": input})
			Expect(r1["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(1)))

			// Step 2: Input A again, output empty.
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": input})
			Expect(r2["out"].IsZero()).To(BeTrue())
		})
	})

	Describe("Reset", func() {
		It("clears all state", func() {
			c := circuit.New("reset-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "out", 0))

			exec, err := New(c, logger.NewZapLogger(logLevel))
			Expect(err).NotTo(HaveOccurred())

			recordA := testutils.Record{ID: "a", Value: 1}
			input := zset.New().WithElems(zset.Elem{Document: recordA, Weight: 1})

			exec.Execute(map[string]zset.ZSet{"in": input})
			exec.Execute(map[string]zset.ZSet{"in": input})

			// Before reset: accumulator = 2.
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": zset.New()})
			Expect(r1["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(2)))

			exec.Reset()

			// After reset: accumulator = 0.
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": input})
			Expect(r2["out"].Lookup(recordA.Hash())).To(Equal(zset.Weight(1)))
		})
	})
})

var _ = Describe("Normal vs Incremental Equivalence", func() {
	// verifyEquivalence verifies the DBSP theorem:
	// D(C(∫(Δs))) = C^Δ(Δs)
	verifyEquivalence := func(normalCircuit *circuit.Circuit, inputSequence []map[string]zset.ZSet) {
		incrCircuit, err := transform.Incrementalize(normalCircuit)
		Expect(err).NotTo(HaveOccurred())

		normalExec, err := New(normalCircuit, logger.NewZapLogger(logLevel))
		Expect(err).NotTo(HaveOccurred())

		incrExec, err := New(incrCircuit, logger.NewZapLogger(logLevel))
		Expect(err).NotTo(HaveOccurred())

		accumulated := make(map[string]zset.ZSet)
		var prevNormalOutput map[string]zset.ZSet

		for round, deltas := range inputSequence {
			// Accumulate inputs (∫(Δs)).
			for id, delta := range deltas {
				if _, exists := accumulated[id]; !exists {
					accumulated[id] = zset.New()
				}
				accumulated[id] = accumulated[id].Add(delta)
			}

			// Run normal circuit on accumulated input.
			// Reset to get fresh state for each complete computation.
			normalExec.Reset()
			normalOutput, err := normalExec.Execute(accumulated)
			Expect(err).NotTo(HaveOccurred(), "round %d normal", round)

			// Run incremental circuit on delta.
			incrOutput, err := incrExec.Execute(deltas)
			Expect(err).NotTo(HaveOccurred(), "round %d incremental", round)

			// Compute expected delta: D(normalOutput) = normalOutput - prevNormalOutput.
			expectedDelta := make(map[string]zset.ZSet)
			for id, out := range normalOutput {
				if prevNormalOutput == nil {
					expectedDelta[id] = out
				} else if prev, exists := prevNormalOutput[id]; !exists {
					expectedDelta[id] = out
				} else {
					expectedDelta[id] = out.Subtract(prev)
				}
			}

			// Verify: incremental output == expected delta.
			for id := range normalOutput {
				Expect(incrOutput[id].Equal(expectedDelta[id])).To(BeTrue(),
					"round %d output %s: got %v, expected %v", round, id, incrOutput[id], expectedDelta[id])
			}

			prevNormalOutput = normalOutput
		}
	}

	Describe("Linear operators", func() {
		It("Select: normal vs incremental", func() {
			c := circuit.New("select-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("sel", operator.NewSelect("gt5", expression.Func(func(ctx *expression.EvalContext) (any, error) {
				e := ctx.Document().(testutils.Record)
				return e.Value > 5, nil
			}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"in": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 3}, Weight: 1})},
				{"in": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "b", Value: 7}, Weight: 1})},
				{"in": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "c", Value: 10}, Weight: 1})},
				{"in": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "b", Value: 7}, Weight: -1})}, // Delete b.
			})
		})

		It("Project: normal vs incremental", func() {
			c := circuit.New("project-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("proj", operator.NewProject("double", expression.Func(func(ctx *expression.EvalContext) (any, error) {
				r := ctx.Document().(testutils.Record)
				return testutils.Record{ID: r.ID, Value: r.Value * 2}, nil
			}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "proj", 0))
			c.AddEdge(circuit.NewEdge("proj", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"in": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 3}, Weight: 1})},
				{"in": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "b", Value: 7}, Weight: 1})},
				{"in": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 3}, Weight: -1})},
			})
		})
	})

	Describe("Bilinear operators", func() {
		It("Cartesian product: normal vs incremental", func() {
			c := circuit.New("product-test")
			c.AddNode(circuit.Input("left"))
			c.AddNode(circuit.Input("right"))
			c.AddNode(circuit.Op("prod", operator.NewCartesianProduct("×")))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("left", "prod", 0))
			c.AddEdge(circuit.NewEdge("right", "prod", 1))
			c.AddEdge(circuit.NewEdge("prod", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"left": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: 1}), "right": zset.New()},
				{"left": zset.New(), "right": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "x", Value: 10}, Weight: 1})},
				{"left": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "b", Value: 2}, Weight: 1}), "right": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "y", Value: 20}, Weight: 1})},
				{"left": zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: -1}), "right": zset.New()},
			})
		})

	})

	Describe("Non-linear operators", func() {
		It("Distinct: normal vs incremental", func() {
			c := circuit.New("distinct-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("dist", operator.NewDistinct("H")))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "dist", 0))
			c.AddEdge(circuit.NewEdge("dist", "out", 0))

			r := testutils.Record{ID: "a", Value: 1}

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"in": zset.New().WithElems(zset.Elem{Document: r, Weight: 1})},  // Weight becomes 1.
				{"in": zset.New().WithElems(zset.Elem{Document: r, Weight: 1})},  // Weight becomes 2, distinct still 1.
				{"in": zset.New().WithElems(zset.Elem{Document: r, Weight: -1})}, // Weight becomes 1, distinct still 1.
				{"in": zset.New().WithElems(zset.Elem{Document: r, Weight: -1})}, // Weight becomes 0, distinct removes.
			})
		})
	})
})

var _ = Describe("Fixed-Point Circuits", func() {
	Describe("Feedback loop", func() {
		// Circuit: in -> plus -> out
		//                  ^  \
		//                  |   v
		//                  +--delay

		It("normal equals incremental", func() {
			c := circuit.New("feedback")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Delay("delay"))
			c.AddNode(circuit.Op("plus", operator.NewPlus()))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("in", "plus", 0))
			c.AddEdge(circuit.NewEdge("delay", "plus", 1))
			c.AddEdge(circuit.NewEdge("plus", "out", 0))
			c.AddEdge(circuit.NewEdge("plus", "delay", 0))

			Expect(c.Validate()).To(BeEmpty())

			// Use verifyEquivalence pattern but adapted for feedback.
			incrCircuit, err := transform.Incrementalize(c)
			Expect(err).NotTo(HaveOccurred())

			normalExec, _ := New(c, logger.NewZapLogger(logLevel))
			incrExec, _ := New(incrCircuit, logger.NewZapLogger(logLevel))

			input := zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: 1})
			empty := map[string]zset.ZSet{"in": zset.New()}

			var prevNormal zset.ZSet
			for round := 0; round < 5; round++ {
				normalOut, _ := normalExec.Execute(map[string]zset.ZSet{"in": input})

				deltas := empty
				if round == 0 {
					deltas = map[string]zset.ZSet{"in": input}
				}
				incrOut, _ := incrExec.Execute(deltas)

				// Expected: D(normal) = normal - prev.
				var expected zset.ZSet
				if prevNormal.IsZero() {
					expected = normalOut["out"]
				} else {
					expected = normalOut["out"].Subtract(prevNormal)
				}

				Expect(incrOut["out"].Equal(expected)).To(BeTrue(),
					"round %d: got %v, expected %v", round, incrOut["out"], expected)

				prevNormal = normalOut["out"].Clone()
			}
		})

		It("stabilizes with empty input", func() {
			c := circuit.New("feedback")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Delay("delay"))
			c.AddNode(circuit.Op("plus", operator.NewPlus()))
			c.AddNode(circuit.Output("out"))

			c.AddEdge(circuit.NewEdge("in", "plus", 0))
			c.AddEdge(circuit.NewEdge("delay", "plus", 1))
			c.AddEdge(circuit.NewEdge("plus", "out", 0))
			c.AddEdge(circuit.NewEdge("plus", "delay", 0))

			incrCircuit, _ := transform.Incrementalize(c)
			incrExec, _ := New(incrCircuit, logger.NewZapLogger(logLevel))

			input := zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: 1})

			// Round 0: provide input.
			out0, _ := incrExec.Execute(map[string]zset.ZSet{"in": input})
			Expect(out0["out"].Size()).To(Equal(1))

			// Round 1: empty input - the feedback produces delta.
			out1, _ := incrExec.Execute(map[string]zset.ZSet{"in": zset.New()})

			// Round 2+: should stabilize (same value circulating = zero delta).
			out2, _ := incrExec.Execute(map[string]zset.ZSet{"in": zset.New()})
			Expect(out2["out"].Equal(out1["out"])).To(BeTrue(), "output should stabilize")
		})
	})
})
