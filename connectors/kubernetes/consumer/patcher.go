package consumer

import (
	"context"
	"fmt"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Patcher applies output deltas to objects it does not own: it maintains
// the fields the pipeline writes on somebody else's objects. It patches
// the merge diff of each (old, new) pair, clears its fields on
// retraction, and never creates or deletes an object: a missing target is
// reported and dropped, because a decorator has no business bringing an
// object back.
type Patcher struct {
	*baseConsumer
}

var _ dbspruntime.Consumer = (*Patcher)(nil)

// NewPatcher creates a patcher consumer.
func NewPatcher(cfg Config) (*Patcher, error) {
	b, err := newBase(cfg, "kubernetes-consumer-patcher")
	if err != nil {
		return nil, err
	}
	b.owns = false
	return &Patcher{baseConsumer: b}, nil
}

// Start runs the consumer event loop, applying each received event with patcher semantics.
func (c *Patcher) Start(ctx context.Context) error {
	return c.start(ctx, c)
}

// Consume applies output events with patcher behavior.
func (c *Patcher) Consume(ctx context.Context, out dbspruntime.Event) error {
	return c.consume(ctx, out)
}

// String implements fmt.Stringer.
func (c *Patcher) String() string {
	return fmt.Sprintf("patcher<k8s>{name=%q topic=%q}", c.Name(), c.outputName)
}
