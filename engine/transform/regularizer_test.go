package transform

import (
	"math/rand"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/internal/testutils"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Regularizer", func() {
	It("injects the regularizer for a single-output circuit", func() {
		c := circuit.New("regularizer")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("noop", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "noop", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("noop", "output_x", 0))).To(Succeed())

		reg, err := NewRegularizer().Transform(c)
		Expect(err).NotTo(HaveOccurred())

		Expect(reg.Node("_sum_output_x")).NotTo(BeNil())
		Expect(reg.Node("_sum_output_x").Kind()).To(Equal(operator.KindLinearCombination))
		Expect(reg.Node("_grp_output_x")).NotTo(BeNil())
		Expect(reg.Node("_grp_output_x").Kind()).To(Equal(operator.KindGroupBy))
		Expect(reg.Node("_reg_output_x")).NotTo(BeNil())
		Expect(reg.Node("_reg_output_x").Kind()).To(Equal(operator.KindProject))
		Expect(reg.Node("_dst_output_x")).To(BeNil())

		Expect(len(reg.EdgesTo("_sum_output_x"))).To(Equal(len(reg.EdgesTo("output_x"))))
		Expect(reg.EdgesTo("_grp_output_x")).To(HaveLen(1))
		Expect(reg.EdgesTo("_grp_output_x")[0].From).To(Equal("_sum_output_x"))

		Expect(reg.EdgesTo("_reg_output_x")).To(HaveLen(1))
		Expect(reg.EdgesTo("_reg_output_x")[0].From).To(Equal("_grp_output_x"))
		Expect(reg.EdgesTo("output_x")).To(HaveLen(1))
		Expect(reg.EdgesTo("output_x")[0].From).To(Equal("_reg_output_x"))

		exec, err := executor.New(reg, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		// Insert a1.
		a1 := testutils.Record{ID: "a", Value: 1}
		step1 := zset.New().WithElems(zset.Elem{Document: a1, Weight: 1})
		out1, err := exec.Execute(map[string]zset.ZSet{"input_x": step1})
		Expect(err).NotTo(HaveOccurred())
		Expect(out1["output_x"].Size()).To(Equal(1))
		Expect(out1["output_x"].Lookup(a1.Hash())).To(Equal(zset.Weight(1)))

		// Delete a1, insert a2.
		a2 := testutils.Record{ID: "a", Value: 2}
		step2 := zset.New().WithElems(
			zset.Elem{Document: a1, Weight: -3},
			zset.Elem{Document: a2, Weight: 5},
		)
		out2, err := exec.Execute(map[string]zset.ZSet{"input_x": step2})
		Expect(err).NotTo(HaveOccurred())
		Expect(out2["output_x"].Size()).To(Equal(1))
		Expect(out2["output_x"].Lookup(a2.Hash())).To(Equal(zset.Weight(1)))

		// Delete a2.
		step3 := zset.New().WithElems(
			zset.Elem{Document: a2, Weight: -1},
		)
		out3, err := exec.Execute(map[string]zset.ZSet{"input_x": step3})
		Expect(err).NotTo(HaveOccurred())
		Expect(out3["output_x"].IsZero()).To(BeTrue())
	})

	It("injects independent regularizer chains for multi-output circuits", func() {
		c := circuit.New("regularizer-multi")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("noop_a", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Op("noop_b", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_a"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_b"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "noop_a", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "noop_b", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("noop_a", "output_a", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("noop_b", "output_b", 0))).To(Succeed())

		reg, err := NewRegularizer().Transform(c)
		Expect(err).NotTo(HaveOccurred())

		for _, out := range []string{"output_a", "output_b"} {
			sumID := "_sum_" + out
			grpID := "_grp_" + out
			regID := "_reg_" + out

			Expect(reg.Node(sumID)).NotTo(BeNil())
			Expect(reg.Node(sumID).Kind()).To(Equal(operator.KindLinearCombination))
			Expect(reg.Node(grpID)).NotTo(BeNil())
			Expect(reg.Node(grpID).Kind()).To(Equal(operator.KindGroupBy))
			Expect(reg.Node(regID)).NotTo(BeNil())
			Expect(reg.Node(regID).Kind()).To(Equal(operator.KindProject))
			Expect(reg.Node("_dst_" + out)).To(BeNil())

			outIn := reg.EdgesTo(out)
			Expect(outIn).To(HaveLen(1))
			Expect(outIn[0].From).To(Equal(regID))
		}

		Expect(reg.Validate()).To(BeEmpty())
	})

	It("deduplicates duplicated upstream rows in snapshot mode", func() {
		c := circuit.New("regularizer-dup")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("a", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Op("b", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "a", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "b", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("a", "output_x", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("b", "output_x", 1))).To(Succeed())

		reg, err := NewRegularizer().Transform(c)
		Expect(err).NotTo(HaveOccurred())

		exec, err := executor.New(reg, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		r := testutils.Record{ID: "a", Value: 1}
		out, err := exec.Execute(map[string]zset.ZSet{"input_x": zset.New().WithElems(zset.Elem{Document: r, Weight: 1})})
		Expect(err).NotTo(HaveOccurred())
		Expect(out["output_x"].Size()).To(Equal(1))
		Expect(out["output_x"].Lookup(r.Hash())).To(Equal(zset.Weight(1)))
	})

	It("preserves delete deltas after incrementalization", func() {
		c := circuit.New("regularizer-inc")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("noop", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "noop", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("noop", "output_x", 0))).To(Succeed())

		reg, err := NewRegularizer().Transform(c)
		Expect(err).NotTo(HaveOccurred())
		incr, err := NewIncrementalizer().Transform(reg)
		Expect(err).NotTo(HaveOccurred())

		exec, err := executor.New(incr, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		r := testutils.Record{ID: "a", Value: 1}
		out1, err := exec.Execute(map[string]zset.ZSet{"input_x": zset.New().WithElems(zset.Elem{Document: r, Weight: 1})})
		Expect(err).NotTo(HaveOccurred())
		Expect(out1["output_x"].Lookup(r.Hash())).To(Equal(zset.Weight(1)))

		out2, err := exec.Execute(map[string]zset.ZSet{"input_x": zset.New().WithElems(zset.Elem{Document: r, Weight: -1})})
		Expect(err).NotTo(HaveOccurred())
		Expect(out2["output_x"].Size()).To(Equal(1))
		Expect(out2["output_x"].Lookup(r.Hash())).To(Equal(zset.Weight(-1)))
	})

	It("matches incrementalized regularizer against snapshot-delta semantics", func() {
		base := circuit.New("regularizer-equivalence")
		Expect(base.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(base.AddNode(circuit.Op("a", operator.NewNoOp()))).To(Succeed())
		Expect(base.AddNode(circuit.Op("b", operator.NewNoOp()))).To(Succeed())
		Expect(base.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(base.AddEdge(circuit.NewEdge("input_x", "a", 0))).To(Succeed())
		Expect(base.AddEdge(circuit.NewEdge("input_x", "b", 0))).To(Succeed())
		Expect(base.AddEdge(circuit.NewEdge("a", "output_x", 0))).To(Succeed())
		Expect(base.AddEdge(circuit.NewEdge("b", "output_x", 1))).To(Succeed())

		reg, err := NewRegularizer().Transform(base)
		Expect(err).NotTo(HaveOccurred())
		incr, err := NewIncrementalizer().Transform(reg)
		Expect(err).NotTo(HaveOccurred())

		normalExec, err := executor.New(reg, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		incrExec, err := executor.New(incr, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		seq := []zset.ZSet{
			zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: 1}),
			zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: -1}),
			zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: 1}),
			zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 2}, Weight: 1}),
			zset.New().WithElems(
				zset.Elem{Document: testutils.Record{ID: "a", Value: 2}, Weight: -1},
				zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: 1},
			),
			zset.New().WithElems(zset.Elem{Document: testutils.Record{ID: "a", Value: 1}, Weight: -1}),
		}

		acc := zset.New()
		prev := zset.New()
		for i, delta := range seq {
			acc = acc.Add(delta)
			normalOut, err := normalExec.Execute(map[string]zset.ZSet{"input_x": acc})
			Expect(err).NotTo(HaveOccurred())
			incrOut, err := incrExec.Execute(map[string]zset.ZSet{"input_x": delta})
			Expect(err).NotTo(HaveOccurred())

			expected := normalOut["output_x"].Subtract(prev)
			Expect(incrOut["output_x"].Equal(expected)).To(BeTrue(),
				"round %d: delta=%v acc=%v got=%v expected=%v", i, delta, acc, incrOut["output_x"], expected)

			prev = normalOut["output_x"].Clone()
		}
	})

	It("handles randomized threshold transitions in incrementalized regularizer", func() {
		base := circuit.New("regularizer-rand")
		Expect(base.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(base.AddNode(circuit.Op("a", operator.NewNoOp()))).To(Succeed())
		Expect(base.AddNode(circuit.Op("b", operator.NewNoOp()))).To(Succeed())
		Expect(base.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(base.AddEdge(circuit.NewEdge("input_x", "a", 0))).To(Succeed())
		Expect(base.AddEdge(circuit.NewEdge("input_x", "b", 0))).To(Succeed())
		Expect(base.AddEdge(circuit.NewEdge("a", "output_x", 0))).To(Succeed())
		Expect(base.AddEdge(circuit.NewEdge("b", "output_x", 1))).To(Succeed())

		reg, err := NewRegularizer().Transform(base)
		Expect(err).NotTo(HaveOccurred())
		incr, err := NewIncrementalizer().Transform(reg)
		Expect(err).NotTo(HaveOccurred())

		normalExec, err := executor.New(reg, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		incrExec, err := executor.New(incr, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		records := []testutils.Record{
			{ID: "a", Value: 1},
			{ID: "b", Value: 2},
			{ID: "c", Value: 3},
		}
		weights := []zset.Weight{-1, 1, -1, 1, -2, 2}
		rng := rand.New(rand.NewSource(20260331))

		acc := zset.New()
		prev := zset.New()
		for round := 0; round < 96; round++ {
			delta := zset.New()
			changes := 2 + rng.Intn(4)
			for i := 0; i < changes; i++ {
				r := records[rng.Intn(len(records))]
				w := weights[rng.Intn(len(weights))]
				delta.Insert(r, w)
			}

			acc = acc.Add(delta)
			normalOut, err := normalExec.Execute(map[string]zset.ZSet{"input_x": acc})
			Expect(err).NotTo(HaveOccurred())
			incrOut, err := incrExec.Execute(map[string]zset.ZSet{"input_x": delta})
			Expect(err).NotTo(HaveOccurred())

			expected := normalOut["output_x"].Subtract(prev)
			Expect(incrOut["output_x"].Equal(expected)).To(BeTrue(),
				"round %d: delta=%v acc=%v got=%v expected=%v", round, delta, acc, incrOut["output_x"], expected)

			prev = normalOut["output_x"].Clone()
		}
	})
})
