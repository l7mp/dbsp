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

	It("injects reconciliation for outputs with multiple incoming edges", func() {
		c := circuit.New("bad-output")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("a", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Op("b", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "a", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("a", "output_x", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "b", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("b", "output_x", 1))).To(Succeed())

		rec, err := NewReconciler(ReconcilerPair{InputID: "input_x", OutputID: "output_x"}).Transform(c)
		Expect(err).NotTo(HaveOccurred())

		sumID := "_rec_output_x_sum"
		Expect(rec.Node(sumID)).NotTo(BeNil())
		Expect(rec.Node(sumID).Kind()).To(Equal(operator.KindLinearCombination))

		outEdges := rec.EdgesTo("output_x")
		Expect(outEdges).To(HaveLen(1))
		Expect(outEdges[0].From).To(Equal("_rec_output_x_acc"))

		sumIn := rec.EdgesTo(sumID)
		Expect(sumIn).To(HaveLen(2))
		Expect(sumIn[0].From).To(Equal("a"))
		Expect(sumIn[1].From).To(Equal("b"))

		subIn := rec.EdgesTo("_rec_output_x_sub")
		Expect(subIn).To(HaveLen(2))
		Expect(subIn[0].From).To(Equal(sumID))
		Expect(subIn[1].From).To(Equal("input_x"))

		Expect(rec.Validate()).To(BeEmpty())
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

	It("produces U = C(deltaY) - deltaY + U[t-1]", func() {
		c := circuit.New("reconciler-exec")
		Expect(c.AddNode(circuit.Input("input_x"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("neg", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_x"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_x", "neg", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("neg", "output_x", 0))).To(Succeed())

		rec, err := NewReconciler().Transform(c)
		Expect(err).NotTo(HaveOccurred())

		exec, err := executor.New(rec, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		a := testutils.Record{ID: "a", Value: 1}
		b := testutils.Record{ID: "b", Value: 3}

		step1 := zset.New().WithElems(zset.Elem{Document: a, Weight: 1})
		out1, err := exec.Execute(map[string]zset.ZSet{"input_x": step1})
		Expect(err).NotTo(HaveOccurred())
		Expect(out1["output_x"].Lookup(a.Hash())).To(Equal(zset.Weight(-2)))

		step2 := zset.New().WithElems(zset.Elem{Document: b, Weight: 1})
		out2, err := exec.Execute(map[string]zset.ZSet{"input_x": step2})
		Expect(err).NotTo(HaveOccurred())

		// U[2] = (-2a) + (-2b).
		Expect(out2["output_x"].Lookup(a.Hash())).To(Equal(zset.Weight(-2)))
		Expect(out2["output_x"].Lookup(b.Hash())).To(Equal(zset.Weight(-2)))
	})
})
