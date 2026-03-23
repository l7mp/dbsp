package runtime

import "fmt"

// Error is a non-critical runtime error with component origin context.
// It is sent on the Runtime error channel and allows callers to observe
// transient errors from individual components without stopping the runtime.
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
