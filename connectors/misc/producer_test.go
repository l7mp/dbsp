package misc

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime/schema"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

var _ = Describe("Virtual source producers", func() {
	It("emits exactly one event for one-shot producer", func() {
		rt := dbspruntime.NewRuntime(logr.Discard())
		p, err := NewOneShotProducer(OneShotConfig{
			Name:       "test-oneshot",
			InputName:  "in",
			TriggerGVK: schema.GroupVersionKind{Group: "test.io", Version: "v1", Kind: "OneShotTrigger"},
			Runtime:    rt,
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
			Name:       "test-periodic",
			InputName:  "in",
			TriggerGVK: schema.GroupVersionKind{Group: "test.io", Version: "v1", Kind: "PeriodicTrigger"},
			Period:     20 * time.Millisecond,
			Runtime:    rt,
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
