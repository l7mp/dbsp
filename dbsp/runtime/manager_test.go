package runtime_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/l7mp/dbsp/dbsp/runtime"
)

func TestManagerAddNil(t *testing.T) {
	t.Parallel()

	m := runtime.NewManager()
	if err := m.Add(nil); !errors.Is(err, runtime.ErrNilRunnable) {
		t.Fatalf("Add(nil) error = %v, want ErrNilRunnable", err)
	}
}

func TestManagerContextCancellation(t *testing.T) {
	t.Parallel()

	m := runtime.NewManager()
	started := make(chan struct{}, 2)

	for range 2 {
		err := m.Add(runnableFunc(func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}))
		if err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Start(ctx)
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for runnable startup")
		}
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Start() return")
	}
}

func TestManagerCancelsOnRunnableError(t *testing.T) {
	t.Parallel()

	m := runtime.NewManager()
	primaryStarted := make(chan struct{})
	primaryExited := make(chan struct{})

	err := m.Add(runnableFunc(func(ctx context.Context) error {
		close(primaryStarted)
		<-ctx.Done()
		close(primaryExited)
		return ctx.Err()
	}))
	if err != nil {
		t.Fatalf("Add(primary) error = %v", err)
	}

	errBoom := errors.New("boom")
	err = m.Add(runnableFunc(func(_ context.Context) error {
		<-primaryStarted
		return errBoom
	}))
	if err != nil {
		t.Fatalf("Add(failing) error = %v", err)
	}

	startErr := m.Start(context.Background())
	if startErr == nil {
		t.Fatal("Start() error = nil, want non-nil")
	}
	if !strings.Contains(startErr.Error(), "boom") {
		t.Fatalf("Start() error = %v, want boom", startErr)
	}

	select {
	case <-primaryExited:
	case <-time.After(2 * time.Second):
		t.Fatal("primary runnable did not observe cancellation")
	}
}

func TestManagerRejectsAddAfterStart(t *testing.T) {
	t.Parallel()

	m := runtime.NewManager()
	started := make(chan struct{})

	err := m.Add(runnableFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}))
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Start(ctx)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runnable startup")
	}

	err = m.Add(runnableFunc(func(context.Context) error { return nil }))
	if !errors.Is(err, runtime.ErrManagerStarted) {
		t.Fatalf("Add() after Start error = %v, want ErrManagerStarted", err)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Start() return")
	}
}

func TestManagerRejectsSecondStart(t *testing.T) {
	t.Parallel()

	m := runtime.NewManager()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Start(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	err := m.Start(context.Background())
	if !errors.Is(err, runtime.ErrManagerStarted) {
		t.Fatalf("second Start() error = %v, want ErrManagerStarted", err)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("first Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first Start() return")
	}
}

func TestManagerRunnableReturnsNilBeforeCancellation(t *testing.T) {
	t.Parallel()

	m := runtime.NewManager()
	err := m.Add(runnableFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	err = m.Start(context.Background())
	if !errors.Is(err, runtime.ErrRunnableReturnedNil) {
		t.Fatalf("Start() error = %v, want ErrRunnableReturnedNil", err)
	}
}

func TestManagerStartWaitsForAllOnShutdown(t *testing.T) {
	t.Parallel()

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
	if err != nil {
		t.Fatalf("Add(one) error = %v", err)
	}

	err = m.Add(runnableFunc(func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		order = append(order, "two")
		mu.Unlock()
		return ctx.Err()
	}))
	if err != nil {
		t.Fatalf("Add(two) error = %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Start(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Start() return")
	}

	mu.Lock()
	gotLen := len(order)
	mu.Unlock()
	if gotLen != 2 {
		t.Fatalf("shutdown completion count = %d, want 2", gotLen)
	}
}

type runnableFunc func(ctx context.Context) error

func (f runnableFunc) Start(ctx context.Context) error {
	return f(ctx)
}
