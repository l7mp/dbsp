package misc

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Virtual source producers", func() {
	It("emits exactly one event for one-shot producer", func() {
		rt := dbspruntime.NewRuntime(logr.Discard())
		p, err := NewOneShotProducer(OneShotConfig{
			Name:        "test-oneshot",
			InputName:   "in",
			TriggerKind: "OneShotTrigger",
			Runtime:     rt,
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		count := 0
		p.SetPublisher(dbspruntime.PublishFunc(func(in dbspruntime.Event) error {
			mu.Lock()
			defer mu.Unlock()
			Expect(in.Name).To(Equal("in"))
			entries := in.Data.Entries()
			Expect(entries).To(HaveLen(1))
			Expect(entries[0].Weight).To(Equal(zset.Weight(1)))
			doc, ok := entries[0].Document.(*dbspunstructured.Unstructured)
			Expect(ok).To(BeTrue())
			Expect(doc.Fields()[VirtualSourceTypeField]).To(Equal(opv1a1OneShotSourceType))
			count++
			return nil
		}))

		done := make(chan error, 1)
		go func() { done <- p.Start(ctx) }()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return count
		}, time.Second, 10*time.Millisecond).Should(Equal(1))

		cancel()
		Eventually(done, time.Second).Should(Receive(BeNil()))
	})

	It("emits repeated events for periodic producer", func() {
		rt := dbspruntime.NewRuntime(logr.Discard())
		p, err := NewPeriodicProducer(PeriodicConfig{
			Name:        "test-periodic",
			InputName:   "in",
			TriggerKind: "PeriodicTrigger",
			Period:      20 * time.Millisecond,
			Runtime:     rt,
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		count := 0
		p.SetPublisher(dbspruntime.PublishFunc(func(in dbspruntime.Event) error {
			mu.Lock()
			defer mu.Unlock()
			Expect(in.Name).To(Equal("in"))
			entries := in.Data.Entries()
			Expect(entries).To(HaveLen(1))
			Expect(entries[0].Weight).To(Equal(zset.Weight(1)))
			doc, ok := entries[0].Document.(*dbspunstructured.Unstructured)
			Expect(ok).To(BeTrue())
			Expect(doc.Fields()[VirtualSourceTypeField]).To(Equal(opv1a1PeriodicSourceType))
			count++
			return nil
		}))

		done := make(chan error, 1)
		go func() { done <- p.Start(ctx) }()

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return count
		}, time.Second, 10*time.Millisecond).Should(BeNumerically(">=", 3))

		cancel()
		Eventually(done, time.Second).Should(Receive(BeNil()))
	})
})
