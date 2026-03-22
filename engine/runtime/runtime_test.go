package runtime_test

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Runtime", func() {
	It("exposes pubsub factories", func() {
		rt := runtime.NewRuntime(logr.Discard())
		Expect(rt.PubSub).NotTo(BeNil())
		Expect(rt.Manager).NotTo(BeNil())

		pub := rt.NewPublisher()
		sub := rt.NewSubscriber()
		Expect(pub).NotTo(BeNil())
		Expect(sub).NotTo(BeNil())
	})

	It("runs added runnable through manager", func() {
		rt := runtime.NewRuntime(logr.Discard())
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

	It("blocks publish when subscriber channel is full", func() {
		rt := runtime.NewRuntime(logr.Discard())
		pub := rt.NewPublisher()
		sub := rt.NewSubscriber()
		sub.Subscribe("topic")

		event := runtime.Event{Name: "topic", Data: zset.New()}
		for i := 0; i < runtime.EventBufferSize; i++ {
			Expect(pub.Publish(event)).To(Succeed())
		}

		done := make(chan error, 1)
		go func() {
			done <- pub.Publish(event)
		}()

		Consistently(done, 200*time.Millisecond, 10*time.Millisecond).ShouldNot(Receive())

		<-sub.GetChannel()

		Eventually(done, time.Second).Should(Receive(BeNil()))
	})
})
