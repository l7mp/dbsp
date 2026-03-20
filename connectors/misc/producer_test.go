package misc

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime/schema"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

var _ = Describe("Virtual source producers", func() {
	It("emits exactly one event for one-shot producer", func() {
		p, err := NewOneShotProducer(OneShotConfig{
			InputName:  "in",
			TriggerGVK: schema.GroupVersionKind{Group: "test.io", Version: "v1", Kind: "OneShotTrigger"},
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		count := 0
		p.SetInputHandler(func(_ context.Context, in dbspruntime.Input) error {
			mu.Lock()
			defer mu.Unlock()
			Expect(in.Name).To(Equal("in"))
			count++
			return nil
		})

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
		p, err := NewPeriodicProducer(PeriodicConfig{
			InputName:  "in",
			TriggerGVK: schema.GroupVersionKind{Group: "test.io", Version: "v1", Kind: "PeriodicTrigger"},
			Period:     20 * time.Millisecond,
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var mu sync.Mutex
		count := 0
		p.SetInputHandler(func(_ context.Context, in dbspruntime.Input) error {
			mu.Lock()
			defer mu.Unlock()
			Expect(in.Name).To(Equal("in"))
			count++
			return nil
		})

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
