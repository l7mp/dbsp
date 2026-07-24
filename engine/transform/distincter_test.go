package transform

import (
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/internal/testutils"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Distincter", func() {
	newCircuit := func() *circuit.Circuit {
		c := circuit.New("distincter")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("noop", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "noop", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("noop", "output_x", 0))).To(Succeed())
		return c
	}

	It("injects a distinct in front of every output", func() {
		reg, err := NewDistincter().Transform(newCircuit())
		Expect(err).NotTo(HaveOccurred())

		// A single predecessor wires into the distinct directly: no fold.
		Expect(reg.Node("_sum_output_x")).To(BeNil())
		Expect(reg.Node("_dst_output_x")).NotTo(BeNil())
		Expect(reg.Node("_dst_output_x").Kind()).To(Equal(operator.KindDistinct))

		Expect(reg.EdgesTo("_dst_output_x")).To(HaveLen(1))
		Expect(reg.EdgesTo("_dst_output_x")[0].From).To(Equal("noop"))
		Expect(reg.EdgesTo("output_x")).To(HaveLen(1))
		Expect(reg.EdgesTo("output_x")[0].From).To(Equal("_dst_output_x"))
		Expect(reg.Validate()).To(BeEmpty())
	})

	It("folds multiple predecessors before the distinct", func() {
		c := circuit.New("distincter-multi")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("a", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Op("b", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "a", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "b", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("a", "output_x", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("b", "output_x", 1))).To(Succeed())

		reg, err := NewDistincter().Transform(c)
		Expect(err).NotTo(HaveOccurred())

		Expect(reg.Node("_sum_output_x")).NotTo(BeNil())
		Expect(reg.Node("_sum_output_x").Kind()).To(Equal(operator.KindLinearCombination))
		Expect(reg.EdgesTo("_sum_output_x")).To(HaveLen(2))
		Expect(reg.EdgesTo("_dst_output_x")).To(HaveLen(1))
		Expect(reg.EdgesTo("_dst_output_x")[0].From).To(Equal("_sum_output_x"))
		Expect(reg.Validate()).To(BeEmpty())
	})

	It("clamps multi-derived outputs to set weights, snapshot and incremental alike", func() {
		a := testutils.Record{ID: "a", Value: 1}

		// Snapshot execution: the output carries weight 1 for a weight-2
		// input, and drops the document when the weight returns to zero.
		reg, err := NewDistincter().Transform(newCircuit())
		Expect(err).NotTo(HaveOccurred())
		exec, err := executor.New(reg, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		out, err := exec.Execute(map[string]zset.ZSet{"input_x": zset.New().WithElems(zset.Elem{Document: a, Weight: 2})})
		Expect(err).NotTo(HaveOccurred())
		Expect(out["output_x"].Lookup(a.Hash())).To(Equal(zset.Weight(1)))

		// Incremental execution: the same, delta-typed. The weight-2
		// assertion emits the document once; retracting one copy changes
		// nothing; retracting the second retracts the document.
		reg2, err := NewDistincter().Transform(newCircuit())
		Expect(err).NotTo(HaveOccurred())
		incr, err := NewIncrementalizer().Transform(reg2)
		Expect(err).NotTo(HaveOccurred())
		iexec, err := executor.New(incr, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		out, err = iexec.Execute(map[string]zset.ZSet{"input_x": zset.New().WithElems(zset.Elem{Document: a, Weight: 2})})
		Expect(err).NotTo(HaveOccurred())
		Expect(out["output_x"].Lookup(a.Hash())).To(Equal(zset.Weight(1)))

		out, err = iexec.Execute(map[string]zset.ZSet{"input_x": zset.New().WithElems(zset.Elem{Document: a, Weight: -1})})
		Expect(err).NotTo(HaveOccurred())
		Expect(out["output_x"].IsZero()).To(BeTrue())

		out, err = iexec.Execute(map[string]zset.ZSet{"input_x": zset.New().WithElems(zset.Elem{Document: a, Weight: -1})})
		Expect(err).NotTo(HaveOccurred())
		Expect(out["output_x"].Lookup(a.Hash())).To(Equal(zset.Weight(-1)))
	})
})
