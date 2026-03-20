package runtime_test

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/l7mp/dbsp/engine/runtime"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Manager", func() {
	It("starts runnables added after start", func() {
		m := runtime.NewManager()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() { errCh <- m.Start(ctx) }()

		started := make(chan struct{}, 1)
		m.Add(runnableFunc(func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return nil
		}))

		Eventually(started, time.Second).Should(Receive())
		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})

	It("stops a runnable via Stop", func() {
		m := runtime.NewManager()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		started := make(chan struct{}, 1)
		stopped := atomic.Bool{}
		r := runnableFunc(func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			stopped.Store(true)
			return nil
		})

		m.Add(r)
		errCh := make(chan error, 1)
		go func() { errCh <- m.Start(ctx) }()

		Eventually(started, time.Second).Should(Receive())
		m.Stop(r)
		Eventually(stopped.Load, time.Second).Should(BeTrue())
		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})
})

type runnableFunc func(ctx context.Context) error

func (f runnableFunc) Start(ctx context.Context) error { return f(ctx) }
