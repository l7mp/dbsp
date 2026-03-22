package runtime

import (
	"sync"

	"github.com/go-logr/logr"
)

// Runtime combines a shared PubSub and a runnable Manager.
type Runtime struct {
	*PubSub
	Manager
	errCh chan<- ComponentError
	log   logr.Logger

	mu sync.RWMutex
}

// NewRuntime creates a Runtime. log is used as a fallback sink for non-critical
// errors when no error channel has been set via SetErrorChannel.
func NewRuntime(log logr.Logger) *Runtime {
	return &Runtime{
		PubSub:  NewPubSub(),
		Manager: NewManager(),
		log:     log,
	}
}

// Add registers r with the manager and starts it if the runtime is already
// running. If r implements Named, its name is registered for uniqueness
// enforcement; a duplicate or empty name causes Add to return an error and the
// component is not added.
func (rt *Runtime) Add(r Runnable) error {
	return rt.Manager.Add(r)
}

// SetErrorChannel wires a channel that receives non-critical ComponentErrors
// from all components registered with this runtime. Must be called before
// Start. The channel should be buffered; a full channel causes the error to be
// dropped and logged instead.
func (rt *Runtime) SetErrorChannel(ch chan<- ComponentError) {
	rt.errCh = ch
}

// ReportError reports a non-critical error from the named component. If an
// error channel has been set, the error is sent non-blocking; a dropped send
// is itself logged. If no channel is set, the error is logged via rt.log.
func (rt *Runtime) ReportError(name string, err error) {
	ce := ComponentError{Origin: name, Err: err}
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
