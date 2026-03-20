package misc

import (
	"context"
	"sync"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// PipeProducer forwards runtime inputs from a channel to an input handler.
type PipeProducer struct {
	in <-chan dbspruntime.Input

	mu      sync.RWMutex
	handler dbspruntime.InputHandler
}

// PipeConsumer forwards runtime outputs to a channel.
type PipeConsumer struct {
	out chan<- dbspruntime.Output
}

var _ dbspruntime.Producer = (*PipeProducer)(nil)
var _ dbspruntime.Consumer = (*PipeConsumer)(nil)

func NewPipeProducer(in <-chan dbspruntime.Input) *PipeProducer {
	return &PipeProducer{in: in}
}

func NewPipeConsumer(out chan<- dbspruntime.Output) *PipeConsumer {
	return &PipeConsumer{out: out}
}

func (p *PipeProducer) SetInputHandler(h dbspruntime.InputHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handler = h
}

func (p *PipeProducer) Start(ctx context.Context) error {
	in := p.in
	for {
		select {
		case <-ctx.Done():
			return nil
		case input, ok := <-in:
			if !ok {
				in = nil
				continue
			}

			p.mu.RLock()
			h := p.handler
			p.mu.RUnlock()
			if h == nil {
				continue
			}

			if err := h(ctx, input); err != nil {
				return err
			}
		}
	}
}

func (c *PipeConsumer) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (c *PipeConsumer) Consume(ctx context.Context, out dbspruntime.Output) error {
	select {
	case c.out <- out:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
