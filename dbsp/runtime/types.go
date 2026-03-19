package runtime

import (
	"context"

	"github.com/l7mp/dbsp/dbsp/zset"
)

// Mode defines the semantic mode of an input/output payload.
type Mode uint8

const (
	// Delta indicates change-based semantics.
	Delta Mode = iota
	// Snapshot indicates state-of-the-world semantics.
	Snapshot
)

// String returns a stable textual representation of Mode.
func (m Mode) String() string {
	switch m {
	case Delta:
		return "delta"
	case Snapshot:
		return "snapshot"
	default:
		return "unknown"
	}
}

// Input is a named payload sent into a runtime endpoint.
type Input struct {
	Name string
	Data zset.ZSet
	Mode Mode
}

// Output is a named payload emitted by a runtime endpoint.
type Output struct {
	Name string
	Data zset.ZSet
	Mode Mode
}

// Runnable is a long-lived component with context-managed lifetime.
//
// Start blocks until completion, context cancellation, or error.
type Runnable interface {
	Start(ctx context.Context) error
}

// Producer emits runtime inputs.
type Producer interface {
	Runnable
	Output() <-chan Input
}

// Consumer receives runtime outputs.
type Consumer interface {
	Runnable
	Input() chan<- Output
}

// Runtime controls the lifecycle of runnables.
type Runtime interface {
	Add(Runnable) error
	Start(ctx context.Context) error
}
