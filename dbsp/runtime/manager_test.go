package runtime_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/l7mp/dbsp/dbsp/runtime"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Manager", func() {
	It("rejects nil runnable", func() {
		m := runtime.NewManager()
		err := m.Add(nil)
		Expect(errors.Is(err, runtime.ErrNilRunnable)).To(BeTrue())
	})

	It("returns nil on context cancellation", func() {
		m := runtime.NewManager()
		started := make(chan struct{}, 2)

		for range 2 {
			err := m.Add(runnableFunc(func(ctx context.Context) error {
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}))
			Expect(err).NotTo(HaveOccurred())
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() { errCh <- m.Start(ctx) }()

		Eventually(started, 2*time.Second).Should(Receive())
		Eventually(started, 2*time.Second).Should(Receive())

		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))
	})

	It("cancels all runnables on first error", func() {
		m := runtime.NewManager()
		primaryStarted := make(chan struct{})
		primaryExited := make(chan struct{})

		err := m.Add(runnableFunc(func(ctx context.Context) error {
			close(primaryStarted)
			<-ctx.Done()
			close(primaryExited)
			return ctx.Err()
		}))
		Expect(err).NotTo(HaveOccurred())

		errBoom := errors.New("boom")
		err = m.Add(runnableFunc(func(_ context.Context) error {
			<-primaryStarted
			return errBoom
		}))
		Expect(err).NotTo(HaveOccurred())

		startErr := m.Start(context.Background())
		Expect(startErr).To(HaveOccurred())
		Expect(strings.Contains(startErr.Error(), "boom")).To(BeTrue())
		Eventually(primaryExited, 2*time.Second).Should(BeClosed())
	})

	It("rejects add after start", func() {
		m := runtime.NewManager()
		started := make(chan struct{})

		err := m.Add(runnableFunc(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}))
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() { errCh <- m.Start(ctx) }()

		Eventually(started, 2*time.Second).Should(BeClosed())

		err = m.Add(runnableFunc(func(context.Context) error { return nil }))
		Expect(errors.Is(err, runtime.ErrManagerStarted)).To(BeTrue())

		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))
	})

	It("rejects second start", func() {
		m := runtime.NewManager()
		started := make(chan struct{})
		err := m.Add(runnableFunc(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}))
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- m.Start(ctx) }()
		Eventually(started, 2*time.Second).Should(BeClosed())

		err = m.Start(context.Background())
		Expect(errors.Is(err, runtime.ErrManagerStarted)).To(BeTrue())

		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))
	})

	It("reports runnable returning nil before cancellation", func() {
		m := runtime.NewManager()
		err := m.Add(runnableFunc(func(context.Context) error { return nil }))
		Expect(err).NotTo(HaveOccurred())

		err = m.Start(context.Background())
		Expect(errors.Is(err, runtime.ErrRunnableReturnedNil)).To(BeTrue())
	})

	It("waits for all runnables on shutdown", func() {
		m := runtime.NewManager()
		ctx, cancel := context.WithCancel(context.Background())

		var mu sync.Mutex
		order := []string{}

		err := m.Add(runnableFunc(func(ctx context.Context) error {
			<-ctx.Done()
			mu.Lock()
			order = append(order, "one")
			mu.Unlock()
			return ctx.Err()
		}))
		Expect(err).NotTo(HaveOccurred())

		err = m.Add(runnableFunc(func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(30 * time.Millisecond)
			mu.Lock()
			order = append(order, "two")
			mu.Unlock()
			return ctx.Err()
		}))
		Expect(err).NotTo(HaveOccurred())

		errCh := make(chan error, 1)
		go func() { errCh <- m.Start(ctx) }()

		time.Sleep(20 * time.Millisecond)
		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))

		mu.Lock()
		gotLen := len(order)
		mu.Unlock()
		Expect(gotLen).To(Equal(2))
	})
})

type runnableFunc func(ctx context.Context) error

func (f runnableFunc) Start(ctx context.Context) error {
	return f(ctx)
}
