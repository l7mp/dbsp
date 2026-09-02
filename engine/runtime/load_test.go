package runtime_test

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/spec"
	"github.com/l7mp/dbsp/engine/zset"
)

func rawJSON(s string) *json.RawMessage {
	m := json.RawMessage(s)
	return &m
}

func groupPtr(s string) *string { return &s }

var _ = Describe("Load", func() {
	It("adapts a transform-less pipeline: snapshot semantics on the delta bus", func() {
		rt := runtime.NewRuntime("adaptertest", logr.Discard())
		reg := runtime.NewConnectorRegistry()
		Expect(reg.Register(runtime.ConnectorFactory{
			Name:   "fake",
			Groups: []string{"fake.connector.dcontroller.io"},
			NewSource: func(rt *runtime.Runtime, s *spec.Source, topic string) (runtime.Runnable, error) {
				return &namedRunnable{name: "fake-src-" + topic, fn: func(ctx context.Context) error {
					<-ctx.Done()
					return nil
				}}, nil
			},
		})).To(Succeed())

		op := &spec.RuntimeSpec{
			Sources: []spec.Source{{Resource: spec.Resource{Group: groupPtr("fake.connector.dcontroller.io"), Kind: "A"}}},
			Circuits: []spec.CircuitSpec{{
				Name:     "snap",
				Inputs:   []string{"A"},
				Outputs:  []string{"Out"},
				Pipeline: rawJSON(`[[{"@inputs": ["A"]}, "@distinct", {"@output": "Out"}]]`),
			}},
		}
		Expect(rt.Load(op, reg)).To(Succeed())

		sub := rt.NewSubscriber()
		sub.Subscribe("Out")
		pub := rt.NewPublisher()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()

		doc := unstructured.New(map[string]any{"metadata": map[string]any{"name": "a"}})
		one := zset.New()
		one.Insert(doc, 1)
		Expect(pub.Publish(runtime.Event{Name: "A", Data: one})).To(Succeed())

		var out runtime.Event
		Eventually(sub.GetChannel(), time.Second).Should(Receive(&out))
		Expect(out.Data.Lookup(doc.Hash())).To(Equal(zset.Weight(1)))

		// A duplicate assertion changes nothing distinct: the adapted
		// circuit recomputes over the input integral and differentiates
		// the unchanged result away. Un-adapted snapshot-on-deltas
		// would emit the document a second time.
		dup := zset.New()
		dup.Insert(doc, 1)
		Expect(pub.Publish(runtime.Event{Name: "A", Data: dup})).To(Succeed())
		Consistently(sub.GetChannel(), 300*time.Millisecond).ShouldNot(Receive())

		// Retracting one of the two copies still leaves the document
		// distinct; retracting the last copy differentiates into the
		// deletion.
		neg := zset.New()
		neg.Insert(doc, -1)
		Expect(pub.Publish(runtime.Event{Name: "A", Data: neg})).To(Succeed())
		Consistently(sub.GetChannel(), 200*time.Millisecond).ShouldNot(Receive())

		neg2 := zset.New()
		neg2.Insert(doc, -1)
		Expect(pub.Publish(runtime.Event{Name: "A", Data: neg2})).To(Succeed())
		Eventually(sub.GetChannel(), time.Second).Should(Receive(&out))
		Expect(out.Data.Lookup(doc.Hash())).To(Equal(zset.Weight(-1)))

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})
})
