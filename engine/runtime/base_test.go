package runtime_test

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Base components", func() {
	It("reports consume errors from BaseConsumer", func() {
		rt := runtime.NewRuntime(logr.Discard())
		errCh := make(chan runtime.Error, 8)
		rt.SetErrorChannel(errCh)

		c, err := runtime.NewBaseConsumer(runtime.BaseConsumerConfig{
			Name:          "base-consumer",
			Subscriber:    rt.NewSubscriber(),
			ErrorReporter: rt,
			Logger:        logr.Discard(),
			Topics:        []string{"in"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(c.String()).To(ContainSubstring(`name="base-consumer"`))
		Expect(c.String()).To(ContainSubstring("topics=[in]"))

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		h := &failingConsumeHandler{err: errors.New("consume failed")}
		go func() {
			done <- c.Run(ctx, h)
		}()

		pub := rt.NewPublisher()
		Expect(pub.Publish(runtime.Event{Name: "in", Data: zset.New()})).To(Succeed())

		var ce runtime.Error
		Eventually(errCh, time.Second).Should(Receive(&ce))
		Expect(ce.Origin).To(Equal("base-consumer"))
		Expect(ce.Error()).To(ContainSubstring(h.err.Error()))

		cancel()
		Eventually(done, time.Second).Should(Receive(BeNil()))
	})

	It("reports consume errors from BaseProcessor", func() {
		rt := runtime.NewRuntime(logr.Discard())
		errCh := make(chan runtime.Error, 8)
		rt.SetErrorChannel(errCh)

		p, err := runtime.NewBaseProcessor(runtime.BaseProcessorConfig{
			Name:          "base-processor",
			Publisher:     rt.NewPublisher(),
			Subscriber:    rt.NewSubscriber(),
			ErrorReporter: rt,
			Logger:        logr.Discard(),
			Topics:        []string{"in"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(p.String()).To(ContainSubstring(`name="base-processor"`))
		Expect(p.String()).To(ContainSubstring("topics=[in]"))

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		h := &failingConsumeHandler{err: errors.New("processor failed")}
		go func() {
			done <- p.Run(ctx, h)
		}()

		pub := rt.NewPublisher()
		Expect(pub.Publish(runtime.Event{Name: "in", Data: zset.New()})).To(Succeed())

		var ce runtime.Error
		Eventually(errCh, time.Second).Should(Receive(&ce))
		Expect(ce.Origin).To(Equal("base-processor"))
		Expect(ce.Error()).To(ContainSubstring(h.err.Error()))

		cancel()
		Eventually(done, time.Second).Should(Receive(BeNil()))
	})

	It("publishes with BaseProducer", func() {
		rt := runtime.NewRuntime(logr.Discard())

		sub := rt.NewSubscriber()
		sub.Subscribe("out")
		defer sub.Unsubscribe("out")

		p, err := runtime.NewBaseProducer(runtime.BaseProducerConfig{
			Name:          "base-producer",
			Publisher:     rt.NewPublisher(),
			ErrorReporter: rt,
			Logger:        logr.Discard(),
			Topics:        []string{"out"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(p.String()).To(ContainSubstring(`name="base-producer"`))
		Expect(p.String()).To(ContainSubstring("topics=[out]"))

		event := runtime.Event{Name: "out", Data: zset.New()}
		Expect(p.Publish(event)).To(Succeed())

		Eventually(sub.GetChannel(), time.Second).Should(Receive(Equal(event)))
	})
})

type failingConsumeHandler struct {
	err error
}

func (h *failingConsumeHandler) Consume(ctx context.Context, event runtime.Event) error {
	return h.err
}
