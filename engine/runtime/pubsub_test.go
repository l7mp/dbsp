package runtime_test

import (
	"github.com/l7mp/dbsp/engine/datamodel"
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

var _ = Describe("PubSub state retention", func() {
	It("replays the retained integral to a late subscriber", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()

		d1, d2 := doc("a", "1"), doc("b", "1")
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: d1, Weight: 1})})).To(Succeed())
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: d2, Weight: 1})})).To(Succeed())

		sub := ps.NewSubscriber()
		sub.Subscribe("t")

		ev, ok := sub.Next()
		Expect(ok).To(BeTrue())
		Expect(ev.Name).To(Equal("t"))
		Expect(ev.Data.Size()).To(Equal(2))
		Expect(ev.Data.Lookup(d1.Hash())).To(Equal(zset.Weight(1)))
		Expect(ev.Data.Lookup(d2.Hash())).To(Equal(zset.Weight(1)))
	})

	It("folds retractions so the replay holds only current state", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()

		old, updated := doc("a", "old"), doc("a", "new")
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: old, Weight: 1})})).To(Succeed())
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(
			zset.Elem{Document: old, Weight: -1},
			zset.Elem{Document: updated, Weight: 1},
		)})).To(Succeed())

		sub := ps.NewSubscriber()
		sub.Subscribe("t")

		ev, ok := sub.Next()
		Expect(ok).To(BeTrue())
		Expect(ev.Data.Size()).To(Equal(1))
		Expect(ev.Data.Lookup(updated.Hash())).To(Equal(zset.Weight(1)))
	})

	It("does not replay when the integral is empty", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()

		d := doc("a", "1")
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: d, Weight: 1})})).To(Succeed())
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: d, Weight: -1})})).To(Succeed())

		sub := ps.NewSubscriber()
		sub.Subscribe("t")

		live := doc("b", "1")
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: live, Weight: 1})})).To(Succeed())

		// The first delivered event is the live delta, not a replay.
		ev, ok := sub.Next()
		Expect(ok).To(BeTrue())
		Expect(ev.Data.Size()).To(Equal(1))
		Expect(ev.Data.Lookup(live.Hash())).To(Equal(zset.Weight(1)))
	})

	It("does not replay to a subscriber on a fresh topic", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()

		sub := ps.NewSubscriber()
		sub.Subscribe("t")

		d := doc("a", "1")
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: d, Weight: 1})})).To(Succeed())

		ev, ok := sub.Next()
		Expect(ok).To(BeTrue())
		Expect(ev.Data.Lookup(d.Hash())).To(Equal(zset.Weight(1)))
	})

	It("delivers replay before subsequent deltas without duplication", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()

		d1 := doc("a", "1")
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: d1, Weight: 1})})).To(Succeed())

		sub := ps.NewSubscriber()
		sub.Subscribe("t")

		d2 := doc("b", "1")
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: d2, Weight: 1})})).To(Succeed())

		// Folding everything the subscriber receives must reproduce the full
		// topic state exactly once.
		acc := zset.New()
		for i := 0; i < 2; i++ {
			ev, ok := sub.Next()
			Expect(ok).To(BeTrue())
			ev.Data.Iter(func(d datamodel.Document, w zset.Weight) bool {
				acc.Insert(d, w)
				return true
			})
		}
		Expect(acc.Size()).To(Equal(2))
		Expect(acc.Lookup(d1.Hash())).To(Equal(zset.Weight(1)))
		Expect(acc.Lookup(d2.Hash())).To(Equal(zset.Weight(1)))
	})

	It("keeps retention across subscriber churn", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()

		d := doc("a", "1")
		Expect(pub.Publish(runtime.Event{Name: "t", Data: delta(zset.Elem{Document: d, Weight: 1})})).To(Succeed())

		first := ps.NewSubscriber()
		first.Subscribe("t")
		_, ok := first.Next()
		Expect(ok).To(BeTrue())
		first.UnsubscribeAll()

		second := ps.NewSubscriber()
		second.Subscribe("t")
		ev, ok := second.Next()
		Expect(ok).To(BeTrue())
		Expect(ev.Data.Lookup(d.Hash())).To(Equal(zset.Weight(1)))
	})

	It("replays independently per topic", func() {
		ps := runtime.NewPubSub()
		pub := ps.NewPublisher()

		d1, d2 := doc("a", "1"), doc("b", "1")
		Expect(pub.Publish(runtime.Event{Name: "t1", Data: delta(zset.Elem{Document: d1, Weight: 1})})).To(Succeed())
		Expect(pub.Publish(runtime.Event{Name: "t2", Data: delta(zset.Elem{Document: d2, Weight: 1})})).To(Succeed())

		sub := ps.NewSubscriber()
		sub.Subscribe("t1")

		ev, ok := sub.Next()
		Expect(ok).To(BeTrue())
		Expect(ev.Name).To(Equal("t1"))
		Expect(ev.Data.Size()).To(Equal(1))
		Expect(ev.Data.Lookup(d1.Hash())).To(Equal(zset.Weight(1)))
	})
})
