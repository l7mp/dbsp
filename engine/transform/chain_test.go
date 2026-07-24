package transform

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/operator"
)

var _ = Describe("Chain", func() {
	pair := ReconcilerPair{InputID: "input_u", OutputID: "output_u"}

	newLoopCircuit := func() *circuit.Circuit {
		c := circuit.New("chain-loop")
		Expect(c.AddNode(circuit.Input("input_d"))).To(Succeed())
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_d", "output_u", 0))).To(Succeed())
		return c
	}

	It("sorts the given transforms into canonical order", func() {
		ch, err := NewChain(
			Spec{Type: Incrementalizer},
			Spec{Type: Distincter},
			Spec{Type: SmithPredictor, Args: []any{2, []ReconcilerPair{pair}}},
		)
		Expect(err).NotTo(HaveOccurred())

		order := make([]TransformerType, 0, len(ch.Specs()))
		for _, s := range ch.Specs() {
			order = append(order, s.Type)
		}
		Expect(order).To(Equal([]TransformerType{SmithPredictor, Distincter, Incrementalizer}))
	})

	It("rejects duplicates and unknown types", func() {
		_, err := NewChain(Spec{Type: Incrementalizer}, Spec{Type: Incrementalizer})
		Expect(err).To(MatchError(ContainSubstring("listed twice")))

		_, err = NewChain(Spec{Type: TransformerType("Bogus")})
		Expect(err).To(MatchError(ContainSubstring("unknown transformer")))

		_, err = NewChain()
		Expect(err).To(MatchError(ContainSubstring("no transforms")))
	})

	It("applies out-of-order specs identically to the manual canonical sequence", func() {
		// Manual: SmithPredictor on the snapshot side, then Incrementalizer.
		manual, err := NewSmithPredictor(2, pair).Transform(newLoopCircuit())
		Expect(err).NotTo(HaveOccurred())
		manual, err = NewIncrementalizer().Transform(manual)
		Expect(err).NotTo(HaveOccurred())

		// Chain, specified backwards.
		ch, err := NewChain(
			Spec{Type: Incrementalizer},
			Spec{Type: SmithPredictor, Args: []any{2, []ReconcilerPair{pair}}},
		)
		Expect(err).NotTo(HaveOccurred())
		chained, err := ch.Transform(newLoopCircuit())
		Expect(err).NotTo(HaveOccurred())

		manualIDs := map[string]bool{}
		for _, n := range manual.Nodes() {
			manualIDs[n.ID] = true
		}
		chainedIDs := map[string]bool{}
		for _, n := range chained.Nodes() {
			chainedIDs[n.ID] = true
		}
		Expect(chainedIDs).To(Equal(manualIDs))
		Expect(chained.Validate()).To(BeEmpty())
	})
})

var _ = Describe("Loop transform guards", func() {
	pair := ReconcilerPair{InputID: "input_u", OutputID: "output_u"}

	newLoopCircuit := func() *circuit.Circuit {
		c := circuit.New("guard-loop")
		Expect(c.AddNode(circuit.Input("input_d"))).To(Succeed())
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("neg", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_d", "neg", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("neg", "output_u", 0))).To(Succeed())
		return c
	}

	It("Reconciler refuses a circuit carrying a Smith loop on the same output", func() {
		sm, err := NewSmithPredictor(2, pair).Transform(newLoopCircuit())
		Expect(err).NotTo(HaveOccurred())

		_, err = NewReconciler(pair).Transform(sm)
		Expect(err).To(MatchError(ContainSubstring("already carries a Smith loop")))
	})

	It("Reconciler refuses to stack on itself", func() {
		rec, err := NewReconciler(pair).Transform(newLoopCircuit())
		Expect(err).NotTo(HaveOccurred())

		_, err = NewReconciler(pair).Transform(rec)
		Expect(err).To(MatchError(ContainSubstring("already carries a Reconciler loop")))
	})
})
