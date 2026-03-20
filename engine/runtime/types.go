package runtime

import (
	"context"

	"github.com/l7mp/dbsp/engine/zset"
)

// Input is a named payload sent into a runtime endpoint.
type Input struct {
	Name string
	Data zset.ZSet
}

// Output is a named payload emitted by a runtime endpoint.
type Output struct {
	Name string
	Data zset.ZSet
}

// AsInput converts an Output into an Input, preserving name and data.
func (o Output) AsInput() Input {
	return Input{Name: o.Name, Data: o.Data}
}

// InputHandler consumes one producer event.
type InputHandler func(context.Context, Input) error

// Runnable is a long-lived component with context-managed lifetime.
//
// Start blocks until completion, context cancellation, or error.
type Runnable interface {
	Start(ctx context.Context) error
}

// Producer emits runtime inputs by invoking a registered input handler.
//
// Producer implementations must call the handler synchronously in their own
// goroutine context when an input event is triggered.
type Producer interface {
	Runnable
	SetInputHandler(InputHandler)
}

// Consumer receives runtime outputs.
//
// Consume executes in the producer-triggered context that ran the circuit step.
// Implementations that need asynchronous handling should buffer internally.
type Consumer interface {
	Runnable
	Consume(ctx context.Context, out Output) error
}

// Manager controls the lifecycle of runnables.
type Manager interface {
	Add(Runnable) error
	Start(ctx context.Context) error
}
