package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

var (
	// ErrManagerStarted indicates that the manager has already been started.
	ErrManagerStarted = errors.New("runtime manager already started")
	// ErrNilRunnable indicates that Add received a nil runnable.
	ErrNilRunnable = errors.New("runtime manager cannot add nil runnable")
	// ErrRunnableReturnedNil indicates that a runnable exited without error before
	// cancellation.
	ErrRunnableReturnedNil = errors.New("runtime runnable returned nil before cancellation")
)

// manager is a one-shot runnable manager.
//
// Start can be called exactly once. Runnables are started concurrently and are
// expected to run until cancellation. If any runnable exits with a non-nil
// error, the manager cancels all others and returns the first error.
type manager struct {
	mu        sync.Mutex
	runnables []Runnable
	started   bool
}

var _ Manager = (*manager)(nil)

// NewManager creates a new lifecycle manager.
func NewManager() Manager {
	return &manager{}
}

// Add registers a runnable. Add must be called before Start.
func (m *manager) Add(r Runnable) error {
	if r == nil {
		return ErrNilRunnable
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return ErrManagerStarted
	}

	m.runnables = append(m.runnables, r)
	return nil
}

// Start launches all registered runnables and blocks until shutdown.
func (m *manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return ErrManagerStarted
	}
	m.started = true
	runnables := append([]Runnable(nil), m.runnables...)
	m.mu.Unlock()

	g, gctx := errgroup.WithContext(ctx)

	// Keep Start blocked until cancellation (or earlier error path).
	g.Go(func() error {
		<-gctx.Done()
		return nil
	})

	for i, r := range runnables {
		i, r := i, r
		g.Go(func() error {
			err := r.Start(gctx)
			if err != nil {
				if isCancellation(err) && gctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("runnable[%d]: %w", i, err)
			}

			if gctx.Err() != nil {
				return nil
			}

			return fmt.Errorf("runnable[%d]: %w", i, ErrRunnableReturnedNil)
		})
	}

	return g.Wait()
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
