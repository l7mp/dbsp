package runtime

import "fmt"

// Error is a runtime error with component origin context. A non-critical
// error is sent on the Runtime error channel and lets callers observe
// transient errors of individual components without stopping the runtime;
// a critical one, a component whose Start failed, is returned by Start and
// stops the runtime.
type Error struct {
	// Origin identifies the component that reported the error, e.g.
	// "circuit/Foo→Bar" or "kubernetes-consumer/my-svc".
	Origin string
	Err    error
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %v", e.Origin, e.Err)
}

func (e Error) Unwrap() error { return e.Err }
