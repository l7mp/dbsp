package runtime

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)

// stopWaitBound caps how long Stop waits for a cancelled component's
// goroutine to return.
const stopWaitBound = 2 * time.Second

// Runnable has a context-managed lifecycle.
type Runnable interface {
	Name() string
	Start(ctx context.Context) error
}

// Manager controls runnable lifecycles, including dynamic add/remove.
type Manager interface {
	Add(Runnable) error
	Stop(Runnable)
	Start(ctx context.Context) error
}

type managed struct {
	r      Runnable
	cancel context.CancelFunc
	done   chan struct{}
}

type manager struct {
	mu      sync.Mutex
	names   map[string]bool
	started bool
	ctx     context.Context
	items   []*managed
	wg      sync.WaitGroup
	errMu   sync.Mutex
	err     error
}

var _ Manager = (*manager)(nil)

// NewManager creates a lifecycle manager that supports dynamic Add and Stop.
func NewManager() Manager {
	return &manager{names: map[string]bool{}}
}

// Add registers a runnable and starts it immediately if the manager is already running.
func (m *manager) Add(r Runnable) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.names[r.Name()]; ok {
		return fmt.Errorf("runtime: component name %q already registered", r.Name())
	}
	m.names[r.Name()] = true

	item := &managed{r: r}
	m.items = append(m.items, item)

	if m.started {
		m.startOneLocked(item)
	}

	return nil
}

// Stop cancels all managed instances that match r and waits, up to a
// bound, for their goroutines to return. The wait is what makes a
// stop-then-replace sequence race-free: a stopped component's teardown
// (closing its publisher and its subscriptions) has
// happened by the time Stop returns, so a successor added right after
// never observes the predecessor's leftovers. A component that ignores
// cancellation is abandoned after the bound with a log line.
func (m *manager) Stop(r Runnable) {
	m.mu.Lock()
	var toStop []*managed
	keep := make([]*managed, 0, len(m.items))
	for _, item := range m.items {
		if sameRunnable(item.r, r) {
			toStop = append(toStop, item)
			// Release the component name so a replacement with the same name can be
			// added later (for example transformed circuits re-registered in-place).
			delete(m.names, item.r.Name())
			continue
		}
		keep = append(keep, item)
	}
	m.items = keep
	m.mu.Unlock()

	for _, item := range toStop {
		if item.cancel != nil {
			item.cancel()
		}
	}
	for _, item := range toStop {
		if item.done == nil {
			continue
		}
		select {
		case <-item.done:
		case <-time.After(stopWaitBound):
			log.Printf("runtime: component %q did not stop within %s, abandoning", item.r.Name(), stopWaitBound)
		}
	}
}

// Start runs until ctx is cancelled and all started runnables have returned.
func (m *manager) Start(ctx context.Context) error {
	m.mu.Lock()
	m.started = true
	m.ctx = ctx
	for _, item := range m.items {
		m.startOneLocked(item)
	}
	m.mu.Unlock()

	<-ctx.Done()
	m.wg.Wait()

	m.errMu.Lock()
	defer m.errMu.Unlock()
	return m.err
}

func sameRunnable(a, b Runnable) bool {
	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)
	if va.Kind() != vb.Kind() {
		return false
	}
	switch va.Kind() {
	case reflect.Func, reflect.Pointer, reflect.UnsafePointer, reflect.Map, reflect.Chan, reflect.Slice:
		return va.Pointer() == vb.Pointer()
	default:
		return reflect.DeepEqual(a, b)
	}
}

func (m *manager) startOneLocked(item *managed) {
	if item.cancel != nil {
		return
	}
	child, cancel := context.WithCancel(m.ctx)
	item.cancel = cancel
	item.done = make(chan struct{})
	m.wg.Add(1)
	go func(r Runnable, done chan struct{}) {
		defer m.wg.Done()
		defer close(done)
		if err := r.Start(child); err != nil {
			m.errMu.Lock()
			if m.err == nil {
				m.err = err
			}
			m.errMu.Unlock()
		}
	}(item.r, item.done)
}
