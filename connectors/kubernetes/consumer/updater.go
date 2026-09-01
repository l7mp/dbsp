package consumer

import (
	"context"
	"fmt"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Updater applies output deltas to objects it owns: objects that exist
// because the pipeline says so. Updates are the same merge patch the
// Patcher sends; ownership shows at the edges: a bare assertion creates
// the object, a bare retraction deletes it, and an update whose target is
// gone recreates it (for a generated object, resurrection is the feature).
type Updater struct {
	*baseConsumer
}

var _ dbspruntime.Consumer = (*Updater)(nil)

// NewUpdater creates an updater consumer.
func NewUpdater(cfg Config) (*Updater, error) {
	b, err := newBase(cfg, "kubernetes-consumer-updater")
	if err != nil {
		return nil, err
	}
	b.owns = true
	b.retryFlush = b.flushLocked
	return &Updater{baseConsumer: b}, nil
}

// Start runs the consumer event loop, applying each received event with updater semantics.
func (c *Updater) Start(ctx context.Context) error {
	return c.start(ctx, c)
}

// Consume applies output events with updater behavior.
func (c *Updater) Consume(ctx context.Context, out dbspruntime.Event) error {
	return c.consume(ctx, out)
}

// String implements fmt.Stringer.
func (c *Updater) String() string {
	return fmt.Sprintf("consumer<k8s-updater>{name=%q, topic=%q}", c.Name(), c.outputName)
}
