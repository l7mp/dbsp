package runtime

import (
	"sync"

	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/engine/executor"
)

// Runtime combines a shared PubSub and a runnable Manager.
type Runtime struct {
	*PubSub
	Manager
	errCh chan<- Error
	log   logr.Logger

	mu        sync.RWMutex
	runnables map[string]Runnable
	observers map[string]observerSetter
}

type observerSetter interface {
	SetObserver(executor.ObserverFunc)
}

// NewRuntime creates a Runtime. log is used as a fallback sink for non-critical
// errors when no error channel has been set via SetErrorChannel.
func NewRuntime(log logr.Logger) *Runtime {
	return &Runtime{
		PubSub:    NewPubSub(),
		Manager:   NewManager(),
		log:       log,
		runnables: map[string]Runnable{},
		observers: map[string]observerSetter{},
	}
}

// Add registers r with the manager and starts it if the runtime is already
// running. If r implements Named, its name is registered for uniqueness
// enforcement; a duplicate or empty name causes Add to return an error and the
// component is not added.
func (rt *Runtime) Add(r Runnable) error {
	if err := rt.Manager.Add(r); err != nil {
		return err
	}

	rt.mu.Lock()
	rt.runnables[r.Name()] = r
	if o, ok := r.(observerSetter); ok {
		rt.observers[r.Name()] = o
	}
	rt.mu.Unlock()

	return nil
}

// Stop unregisters and stops a previously added runnable.
func (rt *Runtime) Stop(r Runnable) {
	rt.mu.Lock()
	delete(rt.runnables, r.Name())
	delete(rt.observers, r.Name())
	rt.mu.Unlock()

	rt.Manager.Stop(r)
}

// SetCircuitObserver installs or clears an observer on a runtime circuit by name.
// It returns true if a runnable with the given name exists and supports observers.
func (rt *Runtime) SetCircuitObserver(name string, observer executor.ObserverFunc) bool {
	rt.mu.RLock()
	target, ok := rt.observers[name]
	rt.mu.RUnlock()
	if !ok {
		return false
	}

	target.SetObserver(observer)
	return true
}

// SetErrorChannel wires a channel that receives non-critical ComponentErrors
// from all components registered with this runtime. Must be called before
// Start. The channel should be buffered; a full channel causes the error to be
// dropped and logged instead.
func (rt *Runtime) SetErrorChannel(ch chan<- Error) {
	rt.errCh = ch
}

// ReportError reports a non-critical error from the named component. If an
// error channel has been set, the error is sent non-blocking; a dropped send
// is itself logged. If no channel is set, the error is logged via rt.log.
func (rt *Runtime) ReportError(name string, err error) {
	ce := Error{Origin: name, Err: err}
	if rt.errCh != nil {
		select {
		case rt.errCh <- ce:
		default:
			rt.log.Error(err, "error channel full, dropping non-critical error", "origin", name)
		}
		return
	}
	rt.log.Error(err, "non-critical error", "origin", name)
}
