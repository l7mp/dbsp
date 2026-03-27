package transform

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

var _ = Describe("Reconciler", func() {
	It("injects the reconciler loop for a single self-referential pair", func() {
		c := circuit.New("rec-test")
		Expect(c.AddNode(circuit.Input("input_pod"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("neg", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_pod"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_pod", "neg", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("neg", "output_pod", 0))).To(Succeed())

		rec, err := NewReconciler(ReconcilerPair{InputID: "input_pod", OutputID: "output_pod"}).Transform(c)
		Expect(err).NotTo(HaveOccurred())

		Expect(rec.Node("_rec_output_pod_sub")).NotTo(BeNil())
		Expect(rec.Node("_rec_output_pod_sub").Kind()).To(Equal(operator.KindLinearCombination))
		Expect(rec.Node("_rec_output_pod_acc")).NotTo(BeNil())
		Expect(rec.Node("_rec_output_pod_acc").Kind()).To(Equal(operator.KindLinearCombination))
		Expect(rec.Node("_rec_output_pod_delay")).NotTo(BeNil())
		Expect(rec.Node("_rec_output_pod_delay").Kind()).To(Equal(operator.KindDelay))

		outEdges := rec.EdgesTo("output_pod")
		Expect(outEdges).To(HaveLen(1))
		Expect(outEdges[0].From).To(Equal("_rec_output_pod_acc"))

		subEdges := rec.EdgesTo("_rec_output_pod_sub")
		Expect(subEdges).To(HaveLen(2))

		Expect(rec.Validate()).To(BeEmpty())
	})

	It("auto-detects self-referential pairs by naming convention", func() {
		c := circuit.New("auto-detect-test")
		Expect(c.AddNode(circuit.Input("input_svc"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("proj", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_svc"))).To(Succeed())
		Expect(c.AddNode(circuit.Input("input_ep"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_lb"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_svc", "proj", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("proj", "output_svc", 0))).To(Succeed())

		rec, err := NewReconciler().Transform(c)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Node("_rec_output_svc_sub")).NotTo(BeNil())
		Expect(rec.Node("_rec_output_ep_sub")).To(BeNil())
		Expect(rec.Node("_rec_output_lb_sub")).To(BeNil())
	})

	It("handles multiple self-referential pairs", func() {
		c := circuit.New("multi-pair")
		Expect(c.AddNode(circuit.Input("input_a"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("op_a", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_a"))).To(Succeed())
		Expect(c.AddNode(circuit.Input("input_b"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("op_b", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_b"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_a", "op_a", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("op_a", "output_a", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_b", "op_b", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("op_b", "output_b", 0))).To(Succeed())

		rec, err := NewReconciler().Transform(c)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.Node("_rec_output_a_sub")).NotTo(BeNil())
		Expect(rec.Node("_rec_output_b_sub")).NotTo(BeNil())
		Expect(rec.Validate()).To(BeEmpty())
	})

	It("errors when output has multiple incoming edges", func() {
		c := circuit.New("bad-output")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("a", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Op("b", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "a", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("a", "output_x", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "b", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("b", "output_x", 0))).To(Succeed())

		_, err := NewReconciler(ReconcilerPair{InputID: "input_x", OutputID: "output_x"}).Transform(c)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("2 incoming edges"))
	})

	It("errors when no pairs found and none specified", func() {
		c := circuit.New("no-pairs")
		Expect(c.AddNode(circuit.Input("in"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("out"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("in", "out", 0))).To(Succeed())

		rec, err := NewReconciler().Transform(c)
		Expect(err).NotTo(HaveOccurred())
		Expect(rec).NotTo(BeNil())
		Expect(rec.Nodes()).To(HaveLen(2))
	})

	It("does not mutate the original circuit", func() {
		c := circuit.New("immutability")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("op", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "op", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("op", "output_x", 0))).To(Succeed())

		originalNodeCount := len(c.Nodes())
		originalEdgeCount := len(c.Edges())

		_, err := NewReconciler().Transform(c)
		Expect(err).NotTo(HaveOccurred())

		Expect(len(c.Nodes())).To(Equal(originalNodeCount))
		Expect(len(c.Edges())).To(Equal(originalEdgeCount))
		Expect(c.Node("_rec_output_x_sub")).To(BeNil())
	})

})
