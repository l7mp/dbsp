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
		var _ runtime.Manager = (*fakeManager)(nil)
		Expect(true).To(BeTrue())
	})
})

type fakeRunnable struct{}

func (f *fakeRunnable) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

type fakeProducer struct {
	fakeRunnable
	h runtime.InputHandler
}

func (f *fakeProducer) SetInputHandler(h runtime.InputHandler) {
	f.h = h
}

type fakeConsumer struct {
	fakeRunnable
	last runtime.Output
}

func (f *fakeConsumer) Consume(_ context.Context, out runtime.Output) error {
	f.last = out
	return nil
}

type fakeManager struct {
	r []runtime.Runnable
}

func (f *fakeManager) Add(r runtime.Runnable) error {
	f.r = append(f.r, r)
	return nil
}

func (f *fakeManager) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
