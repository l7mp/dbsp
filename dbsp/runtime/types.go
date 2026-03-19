package runtime

import (
	"context"

	"github.com/l7mp/dbsp/dbsp/zset"
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

// Processor consumes runtime inputs and emits runtime outputs.
type Processor interface {
	Runnable
	Input() chan<- Input
	Output() <-chan Output
}

// Manager controls the lifecycle of runnables.
type Manager interface {
	Add(Runnable) error
	Start(ctx context.Context) error
}
