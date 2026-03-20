package runtime_test

import (
	"context"
	"time"

	"github.com/l7mp/dbsp/engine/runtime"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Runtime", func() {
	It("exposes pubsub factories", func() {
		rt := runtime.NewRuntime()
		Expect(rt.PubSub).NotTo(BeNil())
		Expect(rt.Manager).NotTo(BeNil())

		pub := rt.NewPublisher()
		sub := rt.NewSubscriber()
		Expect(pub).NotTo(BeNil())
		Expect(sub).NotTo(BeNil())
	})

	It("runs added runnable through manager", func() {
		rt := runtime.NewRuntime()
		started := make(chan struct{}, 1)
		rt.Add(runnableFunc(func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return nil
		}))

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()

		Eventually(started, time.Second).Should(Receive())
		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})
})
