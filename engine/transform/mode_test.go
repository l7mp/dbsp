package transform

import (
	"encoding/json"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
)

// modeTestCircuit builds the identity pipeline (desired deltas flow from
// input_d to output_u, input_u is the feedback), incrementalizes it,
// applies the given loop transform disabled and returns a step function
// that fills the missing inputs with empty Z-sets, and the loop's switch.
func modeTestCircuit(typ TransformerType, args ...any) (func(in map[string]zset.ZSet) map[string]zset.ZSet, *operator.Switch) {
	c := circuit.New("mode-run")
	Expect(c.AddNode(circuit.Input("input_d"))).To(Succeed())
	Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
	Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
	Expect(c.AddEdge(circuit.NewEdge("input_d", "output_u", 0))).To(Succeed())
	incr, err := NewIncrementalizer().Transform(c)
	Expect(err).NotTo(HaveOccurred())
	args = append(args, []ReconcilerPair{{InputID: "input_u", OutputID: "output_u"}}, Enabled(false))
	tr, err := New(typ, args...)
	Expect(err).NotTo(HaveOccurred())
	gated, err := tr.Transform(incr)
	Expect(err).NotTo(HaveOccurred())
	Expect(gated.Validate()).To(BeEmpty())
	var sw *operator.Switch
	for _, n := range gated.Nodes() {
		if s, ok := n.Operator.(*operator.Switch); ok {
			Expect(sw).To(BeNil(), "one switch per loop")
			sw = s
		}
	}
	Expect(sw).NotTo(BeNil())
	Expect(sw.Closed()).To(BeFalse())
	exec, err := executor.New(gated, logr.Discard())
	Expect(err).NotTo(HaveOccurred())
	return func(in map[string]zset.ZSet) map[string]zset.ZSet {
		for _, id := range []string{"input_d", "input_u", "input_g"} {
			if _, ok := in[id]; !ok && gated.Node(id) != nil {
				in[id] = zset.New()
			}
		}
		out, err := exec.Execute(in)
		Expect(err).NotTo(HaveOccurred())
		return out
	}, sw
}

var _ = Describe("Loop switch", func() {
	doc := func(name string) *dbspunstructured.Unstructured {
		return dbspunstructured.New(map[string]any{"name": name})
	}
	delta := func(name string, w zset.Weight) zset.ZSet {
		return zset.New().WithElems(zset.Elem{Document: doc(name), Weight: w})
	}
	weight := func(out map[string]zset.ZSet, name string) zset.Weight {
		return out["output_u"].Lookup(doc(name).Hash())
	}

	It("runs the Reconciler open until the switch closes it, and transfers without a bump", func() {
		step, sw := modeTestCircuit(Reconciler)

		// Open loop: the desired delta goes out once and is never
		// re-emitted, even though nothing confirms it.
		out := step(map[string]zset.ZSet{"input_d": delta("E", 1)})
		Expect(weight(out, "E")).To(Equal(zset.Weight(1)))
		Expect(out["output_u"].Size()).To(Equal(1))
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// The echo of E lands while the loop is still open: the internal
		// correction drains to zero.
		out = step(map[string]zset.ZSet{"input_u": delta("E", 1)})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// A second write, F, is still unconfirmed when the loop closes:
		// closing emits exactly F, the outstanding correction, and nothing
		// for the confirmed E.
		out = step(map[string]zset.ZSet{"input_d": delta("F", 1)})
		Expect(weight(out, "F")).To(Equal(zset.Weight(1)))
		sw.SetClosed(true)
		out = step(map[string]zset.ZSet{})
		Expect(weight(out, "F")).To(Equal(zset.Weight(1)))
		Expect(out["output_u"].Size()).To(Equal(1))

		// Closed loop: F is re-emitted every step until confirmed, then
		// silence.
		out = step(map[string]zset.ZSet{})
		Expect(weight(out, "F")).To(Equal(zset.Weight(1)))
		out = step(map[string]zset.ZSet{"input_u": delta("F", 1)})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// Closed loop: a disturbance (F vanishes from the plant) is
		// corrected at once.
		out = step(map[string]zset.ZSet{"input_u": delta("F", -1)})
		Expect(weight(out, "F")).To(Equal(zset.Weight(1)))
		out = step(map[string]zset.ZSet{"input_u": delta("F", 1)})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// Open again: a new write G goes out once and stays unconfirmed
		// without re-emission.
		sw.SetClosed(false)
		out = step(map[string]zset.ZSet{"input_d": delta("G", 1)})
		Expect(weight(out, "G")).To(Equal(zset.Weight(1)))
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())
	})

	It("closing after a confirmed preload emits nothing", func() {
		step, sw := modeTestCircuit(Reconciler)
		for _, n := range []string{"A", "B", "C"} {
			out := step(map[string]zset.ZSet{"input_d": delta(n, 1)})
			Expect(weight(out, n)).To(Equal(zset.Weight(1)))
		}
		for _, n := range []string{"A", "B", "C"} {
			out := step(map[string]zset.ZSet{"input_u": delta(n, 1)})
			Expect(out["output_u"].IsZero()).To(BeTrue())
		}
		sw.SetClosed(true)
		out := step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())
	})

	It("switches the DualRateSmith the same way, with the window on the internal correction", func() {
		step, sw := modeTestCircuit(DualRateSmith, 1, "g")
		tick := func() zset.ZSet {
			return zset.New().WithElems(zset.Elem{Document: dbspunstructured.New(map[string]any{}), Weight: 1})
		}

		// Open loop: E goes out once. It enters the window on the next
		// step and the prediction absorbs it: the internal correction is
		// zero while E is in flight.
		out := step(map[string]zset.ZSet{"input_d": delta("E", 1)})
		Expect(weight(out, "E")).To(Equal(zset.Weight(1)))
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// Close the loop while E is in flight: nothing to emit.
		sw.SetClosed(true)
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// The window expires with no echo: the lost write is re-emitted
		// once, and re-enters the window.
		out = step(map[string]zset.ZSet{"input_g": tick()})
		Expect(weight(out, "E")).To(Equal(zset.Weight(1)))
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// The echo lands: silence through the drain.
		out = step(map[string]zset.ZSet{"input_u": delta("E", 1)})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		for i := 0; i < 3; i++ {
			out = step(map[string]zset.ZSet{"input_g": tick()})
			Expect(out["output_u"].IsZero()).To(BeTrue(), "drain tick %d", i)
		}
	})

	It("starts closed by default and keeps the Reconciler at its rank", func() {
		pairs := []ReconcilerPair{{InputID: "input_u", OutputID: "output_u"}}
		tr, err := New(Reconciler, pairs)
		Expect(err).NotTo(HaveOccurred())
		Expect(tr.(*reconciler).enabled).To(BeTrue())

		chain, err := NewChain(
			Spec{Type: Reconciler, Args: []any{pairs, Enabled(false)}},
			Spec{Type: Incrementalizer},
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(chain.Specs()[0].Type).To(Equal(Reconciler))
	})

	It("parses the enabled field of the serialized entry", func() {
		off := false
		s, err := TransformSpec{Name: "Reconciler", Pairs: [][]string{{"u", "u"}}, Enabled: &off}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Args).To(ContainElement(Enabled(false)))

		s, err = TransformSpec{Name: "DualRateSmith", Pairs: [][]string{{"u", "u"}}, K: 2, Enabled: &off}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Args).To(ContainElement(Enabled(false)))

		s, err = TransformSpec{Name: "SmithPredictor", Pairs: [][]string{{"u", "u"}}, K: 2, Enabled: &off}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Args).To(ContainElement(Enabled(false)))

		s, err = TransformSpec{Name: "Reconciler", Pairs: [][]string{{"u", "u"}}}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Args).NotTo(ContainElement(Enabled(false)))
	})

	It("round-trips the switch's position through JSON", func() {
		data, err := json.Marshal(operator.NewSwitch(false))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal(`{"type":"switch","open":true}`))
		op, err := operator.UnmarshalOperator(data)
		Expect(err).NotTo(HaveOccurred())
		Expect(op.(*operator.Switch).Closed()).To(BeFalse())

		data, err = json.Marshal(operator.NewSwitch(true))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal(`{"type":"switch"}`))
	})
})
