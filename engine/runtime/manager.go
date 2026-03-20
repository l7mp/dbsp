package runtime

import (
	"context"
	"reflect"
	"sync"
)

// Runnable has a context-managed lifecycle.
type Runnable interface {
	Start(ctx context.Context) error
}

// Manager controls runnable lifecycles, including dynamic add/remove.
type Manager interface {
	Add(Runnable)
	Stop(Runnable)
	Start(ctx context.Context) error
}

type managed struct {
	r      Runnable
	cancel context.CancelFunc
}

type manager struct {
	mu      sync.Mutex
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
	return &manager{}
}

// Add registers a runnable and starts it immediately if the manager is already running.
func (m *manager) Add(r Runnable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	item := &managed{r: r}
	m.items = append(m.items, item)

	if m.started {
		m.startOneLocked(item)
	}
}

// Stop cancels all managed instances that match r.
func (m *manager) Stop(r Runnable) {
	m.mu.Lock()
	var toStop []*managed
	keep := make([]*managed, 0, len(m.items))
	for _, item := range m.items {
		if sameRunnable(item.r, r) {
			toStop = append(toStop, item)
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
	m.wg.Add(1)
	go func(r Runnable) {
		defer m.wg.Done()
		if err := r.Start(child); err != nil {
			m.errMu.Lock()
			if m.err == nil {
				m.err = err
			}
			m.errMu.Unlock()
		}
	}(item.r)
}
