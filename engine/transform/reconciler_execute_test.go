package transform

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/internal/testutils"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Reconciler execution", func() {
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
