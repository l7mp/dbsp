package transform

import (
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
)

// dualRateTestCircuit builds the identity pipeline (desired deltas flow
// from input_d to output_u; input_u is the feedback, input_g the tick the
// transform injects), incrementalizes, applies DualRateSmith with a window
// of k ticks, and returns a step function. Ticks and feedback are injected
// by hand, so the test emulates any interleaving of the two clocks.
func dualRateTestCircuit(k int) func(in map[string]zset.ZSet) map[string]zset.ZSet {
	c := circuit.New("dualrate-run")
	Expect(c.AddNode(circuit.Input("input_d"))).To(Succeed())
	Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
	Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
	Expect(c.AddEdge(circuit.NewEdge("input_d", "output_u", 0))).To(Succeed())

	incr, err := NewIncrementalizer().Transform(c)
	Expect(err).NotTo(HaveOccurred())
	Expect(incr.Validate()).To(BeEmpty())

	drs, err := NewDualRateSmith(k, "g", ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(incr)
	Expect(err).NotTo(HaveOccurred())
	Expect(drs.Validate()).To(BeEmpty())

	exec, err := executor.New(drs, logr.Discard())
	Expect(err).NotTo(HaveOccurred())

	return func(in map[string]zset.ZSet) map[string]zset.ZSet {
		for _, id := range []string{"input_d", "input_u", "input_g"} {
			if _, ok := in[id]; !ok {
				in[id] = zset.New()
			}
		}
		out, err := exec.Execute(in)
		Expect(err).NotTo(HaveOccurred())
		return out
	}
}

var _ = Describe("DualRateSmith", func() {
	// The documents are real Unstructured values so the gate exercises the
	// production merge semantics (merge with the empty tick document must
	// return the command content-identical).
	doc := func(name string) *dbspunstructured.Unstructured {
		return dbspunstructured.New(map[string]any{"name": name})
	}
	delta := func(name string, w zset.Weight) zset.ZSet {
		return zset.New().WithElems(zset.Elem{Document: doc(name), Weight: w})
	}
	// tick is the pulse the misc Tick source emits in pulse mode: a single
	// +1 assertion of one empty document.
	tick := func() zset.ZSet {
		return zset.New().WithElems(zset.Elem{Document: dbspunstructured.New(map[string]any{}), Weight: 1})
	}

	It("injects the loop, the cell chain and its own tick input", func() {
		c := circuit.New("dualrate-wiring")
		Expect(c.AddNode(circuit.Input("input_d"))).To(Succeed())
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_d", "output_u", 0))).To(Succeed())

		drs, err := NewDualRateSmith(2, "", ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(c)
		Expect(err).NotTo(HaveOccurred())

		Expect(drs.Node("input_" + DefaultTickInput)).NotTo(BeNil())
		Expect(drs.Node("input_" + DefaultTickInput).Kind()).To(Equal(operator.KindInput))
		Expect(drs.Node("_drs_output_u_sub")).NotTo(BeNil())
		Expect(drs.Node("_drs_output_u_acc")).NotTo(BeNil())
		Expect(drs.Node("_drs_output_u_delay")).NotTo(BeNil())
		Expect(drs.Node("_drs_output_u_win")).NotTo(BeNil())
		Expect(drs.Node("_drs_output_u_dist").Kind()).To(Equal(operator.KindDistinctH))
		for _, id := range []string{"_drs_output_u_a1", "_drs_output_u_za1", "_drs_output_u_g1",
			"_drs_output_u_a2", "_drs_output_u_za2", "_drs_output_u_g2"} {
			Expect(drs.Node(id)).NotTo(BeNil(), id)
		}
		Expect(drs.Node("_drs_output_u_g1").Kind()).To(Equal(operator.KindCartesian))
		Expect(drs.Validate()).To(BeEmpty())

		outEdges := drs.EdgesTo("output_u")
		Expect(outEdges).To(HaveLen(1))
		Expect(outEdges[0].From).To(Equal("_drs_output_u_acc"))
	})

	It("rejects a window smaller than one tick", func() {
		c := circuit.New("dualrate-k0")
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())

		_, err := NewDualRateSmith(0, "", ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(c)
		Expect(err).To(MatchError(ContainSubstring("need at least 1")))
	})

	It("actuates once and retires the confirmed command through the window", func() {
		step := dualRateTestCircuit(2)
		e := doc("E")

		// The desired delta arrives: emitted once.
		out := step(map[string]zset.ZSet{"input_d": delta("E", 1)})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))

		// A tick while the echo is in flight: the in-flight command
		// shields the loop.
		out = step(map[string]zset.ZSet{"input_g": tick()})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// The echo lands: silence, and from here the confirmed command
		// must drain through the window without resurfacing.
		out = step(map[string]zset.ZSet{"input_u": delta("E", 1)})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		for i := 0; i < 5; i++ {
			out = step(map[string]zset.ZSet{"input_g": tick()})
			Expect(out["output_u"].IsZero()).To(BeTrue(), "drain tick %d", i)
		}
	})

	It("shields the window from event floods and re-detects a lost write by ticks", func() {
		step := dualRateTestCircuit(2)
		e := doc("E")

		// The command whose write will be lost: emitted once.
		out := step(map[string]zset.ZSet{"input_d": delta("E", 1)})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))

		// An event flood with no ticks: unrelated desired changes and
		// their echoes. Event steps must not age the window - the
		// step-clocked Smith would have expired and re-emitted E here.
		for i := 0; i < 6; i++ {
			name := string(rune('a' + i))
			out = step(map[string]zset.ZSet{"input_d": delta(name, 1)})
			Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(0)), "flood step %d", i)
			out = step(map[string]zset.ZSet{"input_u": delta(name, 1)})
			Expect(out["output_u"].IsZero()).To(BeTrue(), "flood echo %d", i)
		}

		// E's echo never arrives; the window is k = 2 ticks, so the
		// second tick releases it, the prediction drops it, and the loop
		// re-emits exactly E - none of the confirmed flood commands.
		out = step(map[string]zset.ZSet{"input_g": tick()})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		out = step(map[string]zset.ZSet{"input_g": tick()})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))
		Expect(out["output_u"].Size()).To(Equal(1))

		// The retry's echo lands; the loop drains to quiescence.
		out = step(map[string]zset.ZSet{"input_u": delta("E", 1)})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		for i := 0; i < 5; i++ {
			out = step(map[string]zset.ZSet{"input_g": tick()})
			Expect(out["output_u"].IsZero()).To(BeTrue(), "drain tick %d", i)
		}
	})

	It("rejects a disturbance at first sight, on the event clock", func() {
		step := dualRateTestCircuit(2)
		e := doc("E")

		// Converge: desire E, tick, echo, drain.
		step(map[string]zset.ZSet{"input_d": delta("E", 1)})
		step(map[string]zset.ZSet{"input_g": tick()})
		step(map[string]zset.ZSet{"input_u": delta("E", 1)})
		for i := 0; i < 4; i++ {
			step(map[string]zset.ZSet{"input_g": tick()})
		}

		// A tamper removes E from the plant: the repair is emitted on the
		// very next step, no tick needed.
		out := step(map[string]zset.ZSet{"input_u": delta("E", -1)})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))

		// The repair's echo retires through the window.
		out = step(map[string]zset.ZSet{"input_g": tick()})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		out = step(map[string]zset.ZSet{"input_u": delta("E", 1)})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		for i := 0; i < 5; i++ {
			out = step(map[string]zset.ZSet{"input_g": tick()})
			Expect(out["output_u"].IsZero()).To(BeTrue(), "drain tick %d", i)
		}
	})

	It("refuses to stack on a Smith loop", func() {
		c := circuit.New("dualrate-stack")
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("neg", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_u", "neg", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("neg", "output_u", 0))).To(Succeed())

		sm, err := NewSmithPredictor(2, ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(c)
		Expect(err).NotTo(HaveOccurred())

		_, err = NewDualRateSmith(2, "", ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(sm)
		Expect(err).To(MatchError(ContainSubstring("already carries a Smith loop")))
	})
})
