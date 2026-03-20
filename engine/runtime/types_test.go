package runtime_test

import (
	"context"

	"github.com/l7mp/dbsp/engine/runtime"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Runtime interfaces", func() {
	It("compile with fake implementations", func() {
		var _ runtime.Runnable = (*fakeRunnable)(nil)
		var _ runtime.Producer = (*fakeProducer)(nil)
		var _ runtime.Consumer = (*fakeConsumer)(nil)
		var _ runtime.Publisher = (*fakePublisher)(nil)
		var _ runtime.Subscriber = (*fakeSubscriber)(nil)
		var _ runtime.Manager = (*fakeManager)(nil)
		Expect(true).To(BeTrue())
	})
})

type fakeRunnable struct{}

func (f *fakeRunnable) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

type fakePublisher struct{}

func (f *fakePublisher) Publish(event runtime.Event) error { return nil }

type fakeProducer struct {
	fakeRunnable
	fakePublisher
}

type fakeSubscriber struct{ ch chan runtime.Event }

func (f *fakeSubscriber) Subscribe(topic string) {}

func (f *fakeSubscriber) Unsubscribe(topic string) {}

func (f *fakeSubscriber) GetChannel() <-chan runtime.Event { return f.ch }

type fakeConsumer struct {
	fakeRunnable
	fakeSubscriber
}

type fakeManager struct{ r []runtime.Runnable }

func (f *fakeManager) Add(r runtime.Runnable) { f.r = append(f.r, r) }

func (f *fakeManager) Stop(runtime.Runnable) {}

func (f *fakeManager) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
