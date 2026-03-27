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

	It("allows re-adding the same name after Stop", func() {
		m := runtime.NewManager()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r1Started := make(chan struct{}, 1)
		r1 := &namedRunnable{name: "same", fn: func(ctx context.Context) error {
			r1Started <- struct{}{}
			<-ctx.Done()
			return nil
		}}

		Expect(m.Add(r1)).To(Succeed())

		errCh := make(chan error, 1)
		go func() { errCh <- m.Start(ctx) }()

		Eventually(r1Started, time.Second).Should(Receive())
		m.Stop(r1)

		r2Started := make(chan struct{}, 1)
		r2 := &namedRunnable{name: "same", fn: func(ctx context.Context) error {
			r2Started <- struct{}{}
			<-ctx.Done()
			return nil
		}}

		Expect(m.Add(r2)).To(Succeed())
		Eventually(r2Started, time.Second).Should(Receive())

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})
})

type runnableFunc func(ctx context.Context) error

func (f runnableFunc) Name() string                    { return "runnableFunc" }
func (f runnableFunc) Start(ctx context.Context) error { return f(ctx) }

type namedRunnable struct {
	name string
	fn   func(context.Context) error
}

func (r *namedRunnable) Name() string                    { return r.name }
func (r *namedRunnable) Start(ctx context.Context) error { return r.fn(ctx) }
