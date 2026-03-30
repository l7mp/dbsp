package transform

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

var _ = Describe("Transformer factory", func() {
	It("creates Incrementalizer", func() {
		t, err := New(Incrementalizer)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Incrementalizer))
	})

	It("creates Rewriter with default rules", func() {
		t, err := New(Rewriter)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Rewriter))
	})

	It("creates Rewriter with named rule set", func() {
		t, err := New(Rewriter, "Post")
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Rewriter))
	})

	It("creates Reconciler with explicit pairs", func() {
		pairs := []ReconcilerPair{{InputID: "in", OutputID: "out"}}
		t, err := New(Reconciler, pairs)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Reconciler))
	})

	It("creates Regularizer", func() {
		t, err := New(Regularizer)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Regularizer))
	})

	It("creates Optimizer", func() {
		t, err := New(Optimizer)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Optimizer))
	})

	It("optimizer applies transforms in canonical order", func() {
		c := circuit.New("x")
		Expect(c.AddNode(circuit.Input("input_foo"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("noop", operator.NewNoOp()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_foo"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_foo", "noop", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("noop", "output_foo", 0))).To(Succeed())

		t, err := New(Optimizer)
		Expect(err).NotTo(HaveOccurred())
		incr, err := t.Transform(c)
		Expect(err).NotTo(HaveOccurred())
		Expect(incr.Name()).To(Equal("x^Δ"))
	})

	It("optimizer rejects already incremental circuits", func() {
		c := circuit.New("already-incremental")
		Expect(c.AddNode(circuit.Input("in"))).To(Succeed())
		Expect(c.AddNode(circuit.Integrate("int"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("out"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("in", "int", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("int", "out", 0))).To(Succeed())

		t, err := New(Optimizer)
		Expect(err).NotTo(HaveOccurred())
		_, err = t.Transform(c)
		Expect(err).To(HaveOccurred())
	})

	It("rejects unknown transformer", func() {
		_, err := New(TransformerType("Bogus"))
		Expect(err).To(HaveOccurred())
	})
})
