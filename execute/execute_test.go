package execute_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/circuit"
	"github.com/l7mp/dbsp/execute"
	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/operator"
	"github.com/l7mp/dbsp/transform"
	"github.com/l7mp/dbsp/zset"
)

func TestExecute(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Execute Suite")
}

// Test element type.
type Record struct {
	ID    string
	Value int
}

func (r Record) Key() string { return r.ID }

func zsetOf(elem zset.Element, weight zset.Weight) zset.ZSet {
	z := zset.New()
	z.Insert(elem, weight)
	return z
}

var _ = Describe("Executor", func() {
	Describe("Basic execution", func() {
		It("executes a simple linear circuit", func() {
			// in -> select (value > 5) -> out.
			c := circuit.New("simple")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("sel", operator.NewSelect("σ", expr.Func(func(e zset.Element) (any, error) {
				return e.(Record).Value > 5, nil
			}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "out", 0))

			exec, err := execute.NewExecutor(c)
			Expect(err).NotTo(HaveOccurred())

			input := zset.New()
			input.Insert(Record{ID: "a", Value: 3}, 1)
			input.Insert(Record{ID: "b", Value: 7}, 1)

			output, err := exec.Execute(map[string]zset.ZSet{"in": input})
			Expect(err).NotTo(HaveOccurred())
			Expect(output["out"].Size()).To(Equal(1))
			Expect(output["out"].Lookup(Record{ID: "b"})).To(Equal(zset.Weight(1)))
		})

		It("handles missing inputs as empty", func() {
			c := circuit.New("missing-input")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "out", 0))

			exec, err := execute.NewExecutor(c)
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

			exec, err := execute.NewExecutor(c)
			Expect(err).NotTo(HaveOccurred())

			// Step 1: Input A, output empty (first step).
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": zsetOf(Record{ID: "a", Value: 1}, 1)})
			Expect(r1["out"].IsZero()).To(BeTrue())

			// Step 2: Input B, output A (previous).
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": zsetOf(Record{ID: "b", Value: 2}, 1)})
			Expect(r2["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(1)))

			// Step 3: Input C, output B.
			r3, _ := exec.Execute(map[string]zset.ZSet{"in": zsetOf(Record{ID: "c", Value: 3}, 1)})
			Expect(r3["out"].Lookup(Record{ID: "b"})).To(Equal(zset.Weight(1)))
		})

		It("integrate accumulates values", func() {
			// in -> integrate -> out.
			c := circuit.New("integrate-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Integrate("int"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "int", 0))
			c.AddEdge(circuit.NewEdge("int", "out", 0))

			exec, err := execute.NewExecutor(c)
			Expect(err).NotTo(HaveOccurred())

			// Step 1: Input A.
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": zsetOf(Record{ID: "a", Value: 1}, 1)})
			Expect(r1["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(1)))

			// Step 2: Input A again (should accumulate).
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": zsetOf(Record{ID: "a", Value: 1}, 1)})
			Expect(r2["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(2)))

			// Step 3: Input B.
			r3, _ := exec.Execute(map[string]zset.ZSet{"in": zsetOf(Record{ID: "b", Value: 2}, 1)})
			Expect(r3["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(2)))
			Expect(r3["out"].Lookup(Record{ID: "b"})).To(Equal(zset.Weight(1)))
		})

		It("differentiate computes difference", func() {
			// in -> differentiate -> out.
			c := circuit.New("diff-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Differentiate("diff"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "diff", 0))
			c.AddEdge(circuit.NewEdge("diff", "out", 0))

			exec, err := execute.NewExecutor(c)
			Expect(err).NotTo(HaveOccurred())

			// Step 1: Input {A:1}, output {A:1} (first step = input).
			input1 := zsetOf(Record{ID: "a", Value: 1}, 1)
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": input1})
			Expect(r1["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(1)))

			// Step 2: Input {A:2}, output {A:1} (2-1=1).
			input2 := zsetOf(Record{ID: "a", Value: 1}, 2)
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": input2})
			Expect(r2["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(1)))

			// Step 3: Input {A:1}, output {A:-1} (1-2=-1).
			input3 := zsetOf(Record{ID: "a", Value: 1}, 1)
			r3, _ := exec.Execute(map[string]zset.ZSet{"in": input3})
			Expect(r3["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(-1)))
		})

		It("delta0 fires only once", func() {
			// in -> delta0 -> out.
			c := circuit.New("delta0-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Delta0("d0"))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "d0", 0))
			c.AddEdge(circuit.NewEdge("d0", "out", 0))

			exec, err := execute.NewExecutor(c)
			Expect(err).NotTo(HaveOccurred())

			input := zsetOf(Record{ID: "a", Value: 1}, 1)

			// Step 1: Input A, output A.
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": input})
			Expect(r1["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(1)))

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

			exec, err := execute.NewExecutor(c)
			Expect(err).NotTo(HaveOccurred())

			input := zsetOf(Record{ID: "a", Value: 1}, 1)

			exec.Execute(map[string]zset.ZSet{"in": input})
			exec.Execute(map[string]zset.ZSet{"in": input})

			// Before reset: accumulator = 2.
			r1, _ := exec.Execute(map[string]zset.ZSet{"in": zset.New()})
			Expect(r1["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(2)))

			exec.Reset()

			// After reset: accumulator = 0.
			r2, _ := exec.Execute(map[string]zset.ZSet{"in": input})
			Expect(r2["out"].Lookup(Record{ID: "a"})).To(Equal(zset.Weight(1)))
		})
	})
})

var _ = Describe("Normal vs Incremental Equivalence", func() {
	// verifyEquivalence verifies the DBSP theorem:
	// D(C(∫(Δs))) = C^Δ(Δs)
	verifyEquivalence := func(normalCircuit *circuit.Circuit, inputSequence []map[string]zset.ZSet) {
		incrCircuit, err := transform.Incrementalize(normalCircuit)
		Expect(err).NotTo(HaveOccurred())

		normalExec, err := execute.NewExecutor(normalCircuit)
		Expect(err).NotTo(HaveOccurred())

		incrExec, err := execute.NewExecutor(incrCircuit)
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
			c.AddNode(circuit.Op("sel", operator.NewSelect("gt5", expr.Func(func(e zset.Element) (any, error) {
				return e.(Record).Value > 5, nil
			}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "sel", 0))
			c.AddEdge(circuit.NewEdge("sel", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"in": zsetOf(Record{ID: "a", Value: 3}, 1)},
				{"in": zsetOf(Record{ID: "b", Value: 7}, 1)},
				{"in": zsetOf(Record{ID: "c", Value: 10}, 1)},
				{"in": zsetOf(Record{ID: "b", Value: 7}, -1)}, // Delete b.
			})
		})

		It("Project: normal vs incremental", func() {
			c := circuit.New("project-test")
			c.AddNode(circuit.Input("in"))
			c.AddNode(circuit.Op("proj", operator.NewProject("double", expr.Func(func(e zset.Element) (any, error) {
				r := e.(Record)
				return Record{ID: r.ID, Value: r.Value * 2}, nil
			}))))
			c.AddNode(circuit.Output("out"))
			c.AddEdge(circuit.NewEdge("in", "proj", 0))
			c.AddEdge(circuit.NewEdge("proj", "out", 0))

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"in": zsetOf(Record{ID: "a", Value: 3}, 1)},
				{"in": zsetOf(Record{ID: "b", Value: 7}, 1)},
				{"in": zsetOf(Record{ID: "a", Value: 3}, -1)},
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
				{"left": zsetOf(Record{ID: "a", Value: 1}, 1), "right": zset.New()},
				{"left": zset.New(), "right": zsetOf(Record{ID: "x", Value: 10}, 1)},
				{"left": zsetOf(Record{ID: "b", Value: 2}, 1), "right": zsetOf(Record{ID: "y", Value: 20}, 1)},
				{"left": zsetOf(Record{ID: "a", Value: 1}, -1), "right": zset.New()},
			})
		})

		It("Join: normal vs incremental", func() {
			predicate := expr.Func(func(e zset.Element) (any, error) {
				p := e.(*operator.Pair)
				left := p.Left().(Record)
				right := p.Right().(Record)
				return left.Value == right.Value, nil
			})

			c := circuit.Join("join-test", predicate)

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"left": zsetOf(Record{ID: "a", Value: 1}, 1), "right": zset.New()},
				{"left": zset.New(), "right": zsetOf(Record{ID: "x", Value: 1}, 1)},
				{"left": zsetOf(Record{ID: "b", Value: 2}, 1), "right": zsetOf(Record{ID: "y", Value: 2}, 1)},
				{"left": zsetOf(Record{ID: "a", Value: 1}, -1), "right": zset.New()},
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

			r := Record{ID: "a", Value: 1}

			verifyEquivalence(c, []map[string]zset.ZSet{
				{"in": zsetOf(r, 1)},  // Weight becomes 1.
				{"in": zsetOf(r, 1)},  // Weight becomes 2, distinct still 1.
				{"in": zsetOf(r, -1)}, // Weight becomes 1, distinct still 1.
				{"in": zsetOf(r, -1)}, // Weight becomes 0, distinct removes.
			})
		})
	})
})
