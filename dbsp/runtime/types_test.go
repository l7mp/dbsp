package runtime_test

import (
	"context"
	"testing"

	"github.com/l7mp/dbsp/dbsp/runtime"
)

func TestInterfacesCompile(t *testing.T) {
	t.Parallel()

	var _ runtime.Runnable = (*fakeRunnable)(nil)
	var _ runtime.Producer = (*fakeProducer)(nil)
	var _ runtime.Consumer = (*fakeConsumer)(nil)
	var _ runtime.Manager = (*fakeManager)(nil)
}

type fakeRunnable struct{}

func (f *fakeRunnable) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

type fakeProducer struct {
	fakeRunnable
	out chan runtime.Input
}

func (f *fakeProducer) Output() <-chan runtime.Input {
	return f.out
}

type fakeConsumer struct {
	fakeRunnable
	in chan runtime.Output
}

func (f *fakeConsumer) Input() chan<- runtime.Output {
	return f.in
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
