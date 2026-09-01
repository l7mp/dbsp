package runtime_test

import (
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func doc(name, value string) *unstructured.Unstructured {
	return unstructured.New(map[string]any{"name": name, "value": value})
}

func delta(entries ...zset.Elem) zset.ZSet {
	zs := zset.New()
	for _, e := range entries {
		zs.Insert(e.Document, e.Weight)
	}
	return zs
}

var _ = Describe("PubSub queue introspection", func() {
	It("reports the number of queued events", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()

		sub := ps.NewSubscriber()
		sub.Subscribe("t")
		Expect(sub.QueueSize()).To(Equal(0))

		for i := 0; i < 3; i++ {
			Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: doc("a", "1"), Weight: 1})})).To(Succeed())
		}
		Expect(sub.QueueSize()).To(Equal(3))

		_, ok := sub.Next()
		Expect(ok).To(BeTrue())
		Expect(sub.QueueSize()).To(Equal(2))
	})
})

var _ = Describe("PubSub fan-out", func() {
	It("delivers events only to subscribers registered at publish time", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: doc("a", "1"), Weight: 1})})).To(Succeed())

		// A late subscriber gets nothing: the pub/sub is pure transport.
		sub := ps.NewSubscriber()
		sub.Subscribe("t")
		Expect(sub.QueueSize()).To(Equal(0))

		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: doc("b", "1"), Weight: 1})})).To(Succeed())
		ev, ok := sub.Next()
		Expect(ok).To(BeTrue())
		Expect(ev.Data.Lookup(doc("b", "1").Hash())).To(Equal(zset.Weight(1)))
	})

	It("refuses publishes through a closed publisher", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()
		pub.Close()

		err := pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: doc("a", "1"), Weight: 1})})
		Expect(err).To(MatchError(runtime.ErrPublisherClosed))
	})
})
