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

// smithTestCircuit builds the identity pipeline (desired deltas flow from
// input_d to output_u; input_u is the feedback the driver injects by hand),
// applies SmithPredictor with window k, incrementalizes, and returns an
// executor plus a step function that plays one circuit step. Injecting the
// feedback manually lets the test emulate any true dead time.
func smithTestCircuit(k int) func(in map[string]zset.ZSet) map[string]zset.ZSet {
	c := circuit.New("smith-run")
	Expect(c.AddNode(circuit.Input("input_d"))).To(Succeed())
	Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
	Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
	Expect(c.AddEdge(circuit.NewEdge("input_d", "output_u", 0))).To(Succeed())

	sm, err := NewSmithPredictor(k, ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(c)
	Expect(err).NotTo(HaveOccurred())
	Expect(sm.Validate()).To(BeEmpty())

	incr, err := NewIncrementalizer().Transform(sm)
	Expect(err).NotTo(HaveOccurred())
	Expect(incr.Validate()).To(BeEmpty())

	exec, err := executor.New(incr, logr.Discard())
	Expect(err).NotTo(HaveOccurred())

	return func(in map[string]zset.ZSet) map[string]zset.ZSet {
		// Input ops replay their stored value until re-Set, so a silent
		// step must set every input to the empty delta explicitly.
		for _, id := range []string{"input_d", "input_u"} {
			if _, ok := in[id]; !ok {
				in[id] = zset.New()
			}
		}
		out, err := exec.Execute(in)
		Expect(err).NotTo(HaveOccurred())
		return out
	}
}

var _ = Describe("SmithPredictor", func() {
	e := testutils.Record{ID: "E", Value: 1}
	plus := func() zset.ZSet { return zset.New().WithElems(zset.Elem{Document: e, Weight: 1}) }
	minus := func() zset.ZSet { return zset.New().WithElems(zset.Elem{Document: e, Weight: -1}) }

	It("injects the compensated loop for an explicit pair", func() {
		c := circuit.New("smith-wiring")
		Expect(c.AddNode(circuit.Input("input_d"))).To(Succeed())
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_d", "output_u", 0))).To(Succeed())

		sm, err := NewSmithPredictor(3, ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(c)
		Expect(err).NotTo(HaveOccurred())

		// The reconciler core, the window delay line (k cells: the acc
		// feedback delay plus k−1 chained), and the prediction distinct.
		Expect(sm.Node("_smith_output_u_sub")).NotTo(BeNil())
		Expect(sm.Node("_smith_output_u_acc")).NotTo(BeNil())
		Expect(sm.Node("_smith_output_u_delay")).NotTo(BeNil())
		Expect(sm.Node("_smith_output_u_w2")).NotTo(BeNil())
		Expect(sm.Node("_smith_output_u_w3")).NotTo(BeNil())
		Expect(sm.Node("_smith_output_u_win")).NotTo(BeNil())
		Expect(sm.Node("_smith_output_u_dist")).NotTo(BeNil())
		Expect(sm.Node("_smith_output_u_dist").Kind()).To(Equal(operator.KindDistinct))

		outEdges := sm.EdgesTo("output_u")
		Expect(outEdges).To(HaveLen(1))
		Expect(outEdges[0].From).To(Equal("_smith_output_u_acc"))

		Expect(sm.Validate()).To(BeEmpty())
	})

	It("rejects a window smaller than 2", func() {
		c := circuit.New("smith-k1")
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())

		_, err := NewSmithPredictor(1, ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(c)
		Expect(err).To(MatchError(ContainSubstring("k = 1 is the Reconciler")))
	})

	It("refuses to stack on a reconciled output", func() {
		c := circuit.New("smith-stack")
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Op("neg", operator.NewNegate()))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_u", "neg", 0))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("neg", "output_u", 0))).To(Succeed())

		rec, err := NewReconciler(ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(c)
		Expect(err).NotTo(HaveOccurred())

		_, err = NewSmithPredictor(2, ReconcilerPair{InputID: "input_u", OutputID: "output_u"}).Transform(rec)
		Expect(err).To(MatchError(ContainSubstring("already carries a Reconciler loop")))
	})

	It("refuses to stack on itself", func() {
		c := circuit.New("smith-restack")
		Expect(c.AddNode(circuit.Input("input_d"))).To(Succeed())
		Expect(c.AddNode(circuit.Input("input_u"))).To(Succeed())
		Expect(c.AddNode(circuit.Output("output_u"))).To(Succeed())
		Expect(c.AddEdge(circuit.NewEdge("input_d", "output_u", 0))).To(Succeed())

		pair := ReconcilerPair{InputID: "input_u", OutputID: "output_u"}
		sm, err := NewSmithPredictor(2, pair).Transform(c)
		Expect(err).NotTo(HaveOccurred())

		_, err = NewSmithPredictor(2, pair).Transform(sm)
		Expect(err).To(MatchError(ContainSubstring("already carries a Smith loop")))
	})

	It("executes the worked run at exact dead time: single-shot actuation and disturbance rejection", func() {
		// k = K = 2: Y_fb[t] = X[t−1]; the echo of an emission at t is the
		// feedback delta at t+2.
		step := smithTestCircuit(2)

		// t=0: the desired delta +E arrives; emitted once.
		out := step(map[string]zset.ZSet{"input_d": plus()})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))

		// t=1: the in-flight command shields the loop (the plain Reconciler
		// would re-emit here).
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// t=2: the command leaves the window in the very step its echo
		// lands: the prediction never moves.
		out = step(map[string]zset.ZSet{"input_u": plus()})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// t=3..4: a disturbance removes E; not yet observable, silent.
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// t=5: the disturbance surfaces: news, corrected at first sight.
		out = step(map[string]zset.ZSet{"input_u": minus()})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))

		// t=6: the repair rides the window.
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// t=7: the repair's echo meets its expiry: quiescent.
		out = step(map[string]zset.ZSet{"input_u": plus()})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// A retraction round trip: the desired state drops E.
		out = step(map[string]zset.ZSet{"input_d": minus()})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(-1)))
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		out = step(map[string]zset.ZSet{"input_u": minus()})
		Expect(out["output_u"].IsZero()).To(BeTrue())
	})

	It("executes the crossing run: +E then -E both in flight, both echoes silent", func() {
		// k = K = 2. The desired state adds E at t=0 and drops it at t=1,
		// while the create command is still in flight. The window holds the
		// commands as a signed sum, but only as linear superposition inside
		// the prediction, where the transient annihilation is exactly the
		// pair's net predicted effect; expiry is temporal, so each entry
		// leaves the window in the very step its own echo lands.
		step := smithTestCircuit(2)

		// t=0: create commanded.
		out := step(map[string]zset.ZSet{"input_d": plus()})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))

		// t=1: the crossing: the desired state retracts E mid-flight. The
		// prediction realizes the in-flight create, so the retraction is
		// commanded once.
		out = step(map[string]zset.ZSet{"input_d": minus()})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(-1)))

		// t=2: the create's echo lands as the create expires: silence.
		out = step(map[string]zset.ZSet{"input_u": plus()})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// t=3: the delete's echo lands as the delete expires: silence,
		// quiescent.
		out = step(map[string]zset.ZSet{"input_u": minus()})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())
	})

	It("executes the under-estimated run: re-emission every K steps, the overlap collapsed", func() {
		// True dead time k = 3 against window K = 2: the echo of an
		// emission at t is the feedback delta at t+3, so the command
		// expires unconfirmed once and the loop re-emits: ⌈3/2⌉ = 2
		// actuations against the reconciler's 3.
		step := smithTestCircuit(2)

		// t=0: emitted.
		out := step(map[string]zset.ZSet{"input_d": plus()})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))

		// t=1: shielded by the window.
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// t=2: expired unconfirmed (the echo is one step away): re-emitted.
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].Lookup(e.Hash())).To(Equal(zset.Weight(1)))

		// t=3: the original emission's echo lands while the re-emission is
		// in the window; the distinct collapses the overlap. Without it the
		// loop would emit a spurious retraction here.
		out = step(map[string]zset.ZSet{"input_u": plus()})
		Expect(out["output_u"].IsZero()).To(BeTrue())

		// t=4: the re-emission expires against the observation: silent,
		// quiescent. (The plant absorbed the duplicate; no further echo.)
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())
		out = step(map[string]zset.ZSet{})
		Expect(out["output_u"].IsZero()).To(BeTrue())
	})
})
