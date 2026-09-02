package runtime_test

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/compiler"
	aggcompiler "github.com/l7mp/dbsp/engine/compiler/aggregation"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
)

// distinctQuery compiles the minimal nonlinear pipeline A -> @distinct ->
// Out; nonlinearity makes the snapshot-vs-incremental equivalence
// non-trivial.
func distinctQuery() *compiler.Query {
	q, err := aggcompiler.New(
		[]aggcompiler.Binding{{Name: "A", Logical: "A"}},
		[]aggcompiler.Binding{{Name: "Out", Logical: "Out"}},
	).CompileString(`[[{"@inputs": ["A"]}, "@distinct", {"@output": "Out"}]]`)
	Expect(err).NotTo(HaveOccurred())
	return q
}

// startRuntime commits the query with the given transforms and options and
// starts the runtime; the returned stop function tears it down.
func startRuntime(name string, specs []transform.TransformSpec, opts runtime.CommitOptions) (*runtime.Runtime, runtime.Publisher, <-chan runtime.Event, func()) {
	rt := runtime.NewRuntime(name, logr.Discard())
	_, err := rt.CommitCircuit(name, distinctQuery(), specs, opts)
	Expect(err).NotTo(HaveOccurred())

	sub := rt.NewSubscriber()
	sub.Subscribe("Out")
	pub := rt.NewPublisher()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- rt.Start(ctx) }()
	return rt, pub, sub.GetChannel(), func() {
		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	}
}

func docOf(name string) *unstructured.Unstructured {
	return unstructured.New(map[string]any{"metadata": map[string]any{"name": name}})
}

func deltaOf(entries map[*unstructured.Unstructured]int) zset.ZSet {
	z := zset.New()
	for doc, w := range entries {
		z.Insert(doc, zset.Weight(w))
	}
	return z
}

var _ = Describe("CommitCircuit", func() {
	a, b := docOf("a"), docOf("b")

	// driveDistinct feeds the canonical sequence and returns the weights
	// the output stream carried for a and b at each delivered step.
	driveDistinct := func(pub runtime.Publisher, ch <-chan runtime.Event) [][2]int64 {
		recv := func() [2]int64 {
			var out runtime.Event
			Eventually(ch, time.Second).Should(Receive(&out))
			return [2]int64{int64(out.Data.Lookup(a.Hash())), int64(out.Data.Lookup(b.Hash()))}
		}
		quiet := func() {
			Consistently(ch, 200*time.Millisecond).ShouldNot(Receive())
		}

		steps := [][2]int64{}
		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{a: 1})})).To(Succeed())
		steps = append(steps, recv())
		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{b: 1})})).To(Succeed())
		steps = append(steps, recv())
		// The duplicate changes nothing distinct: no emission.
		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{a: 1})})).To(Succeed())
		quiet()
		// Retracting one of two copies changes nothing; the last copy
		// differentiates into the deletion.
		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{a: -1})})).To(Succeed())
		quiet()
		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{a: -1})})).To(Succeed())
		steps = append(steps, recv())
		return steps
	}

	It("computes the same operator with default adapters and with the Incrementalizer", func() {
		// The theorem behind the commit contract, Q^Δ = D ∘ Q ∘ ∫: the
		// plain commit (snapshot adapters) and the incrementalized commit
		// must be indistinguishable on the bus.
		_, pubA, chA, stopA := startRuntime("commit-adapted", nil, runtime.CommitOptions{Compiled: true})
		defer stopA()
		_, pubI, chI, stopI := startRuntime("commit-incremental",
			[]transform.TransformSpec{{Name: "Incrementalizer"}}, runtime.CommitOptions{Compiled: true})
		defer stopI()

		adapted := driveDistinct(pubA, chA)
		incremental := driveDistinct(pubI, chI)
		Expect(adapted).To(Equal(incremental))
		Expect(adapted).To(Equal([][2]int64{{1, 0}, {0, 1}, {-1, 0}}))
	})

	It("leaves a hand-built circuit raw by default", func() {
		// Compiled == false: no adapters. The snapshot program runs on the
		// raw deltas, so the duplicate insert IS emitted again - exactly
		// the behavior the default protects compiled circuits from.
		_, pub, sub, stop := startRuntime("commit-raw", nil, runtime.CommitOptions{})
		defer stop()

		var out runtime.Event
		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{a: 1})})).To(Succeed())
		Eventually(sub, time.Second).Should(Receive(&out))
		Expect(out.Data.Lookup(a.Hash())).To(Equal(zset.Weight(1)))

		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{a: 1})})).To(Succeed())
		Eventually(sub, time.Second).Should(Receive(&out))
		Expect(out.Data.Lookup(a.Hash())).To(Equal(zset.Weight(1)))
	})

	It("honors the positive adapter lists literally", func() {
		// Inputs adapted, output raw: the circuit recomputes over the
		// integrated state and ships the LEVEL - the second insert emits
		// both documents.
		ins := []string{"A"}
		_, pub, sub, stop := startRuntime("commit-partial", nil,
			runtime.CommitOptions{AdaptInputs: &ins, AdaptOutputs: &[]string{}})
		defer stop()

		var out runtime.Event
		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{a: 1})})).To(Succeed())
		Eventually(sub, time.Second).Should(Receive(&out))
		Expect(out.Data.Lookup(a.Hash())).To(Equal(zset.Weight(1)))

		Expect(pub.Publish(runtime.Event{Name: "A", Data: deltaOf(map[*unstructured.Unstructured]int{b: 1})})).To(Succeed())
		Eventually(sub, time.Second).Should(Receive(&out))
		Expect(out.Data.Lookup(a.Hash())).To(Equal(zset.Weight(1)))
		Expect(out.Data.Lookup(b.Hash())).To(Equal(zset.Weight(1)))
	})

	It("rejects an unknown boundary name in the adapter lists", func() {
		rt := runtime.NewRuntime("commit-badlist", logr.Discard())
		bad := []string{"NoSuchInput"}
		_, err := rt.CommitCircuit("commit-badlist", distinctQuery(), nil,
			runtime.CommitOptions{Compiled: true, AdaptInputs: &bad})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unknown input"))
	})

	It("accepts explicit adapters alongside the Incrementalizer", func() {
		// Explicit lists are honored literally - the caller owns the
		// boundary configuration, however partial the optimization ends up.
		rt := runtime.NewRuntime("commit-inc-adapted", logr.Discard())
		ins := []string{"A"}
		_, err := rt.CommitCircuit("commit-inc-adapted", distinctQuery(),
			[]transform.TransformSpec{{Name: "Incrementalizer"}},
			runtime.CommitOptions{Compiled: true, AdaptInputs: &ins})
		Expect(err).NotTo(HaveOccurred())
	})
})
