package misc

import (
	"context"
	"sync"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// PipeProducer forwards runtime inputs from a channel to an input handler.
type PipeProducer struct {
	name string
	in   <-chan dbspruntime.Event

	mu        sync.RWMutex
	publisher dbspruntime.Publisher
}

// PipeConsumer forwards runtime outputs to a channel.
type PipeConsumer struct {
	name string
	out  chan<- dbspruntime.Event
	in   chan dbspruntime.Event
}

var _ dbspruntime.Producer = (*PipeProducer)(nil)
var _ dbspruntime.Consumer = (*PipeConsumer)(nil)

func NewPipeProducer(name string, in <-chan dbspruntime.Event) *PipeProducer {
	return &PipeProducer{name: name, in: in}
}

func NewPipeConsumer(name string, out chan<- dbspruntime.Event) *PipeConsumer {
	return &PipeConsumer{name: name, out: out, in: make(chan dbspruntime.Event, dbspruntime.EventBufferSize)}
}

func (p *PipeProducer) Name() string { return p.name }
func (p *PipeProducer) SetPublisher(pub dbspruntime.Publisher) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publisher = pub
}

func (p *PipeProducer) Publish(event dbspruntime.Event) error {
	p.mu.RLock()
	pub := p.publisher
	p.mu.RUnlock()
	if pub == nil {
		return nil
	}
	return pub.Publish(event)
}

func (p *PipeProducer) Start(ctx context.Context) error {
	in := p.in
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-in:
			if !ok {
				in = nil
				continue
			}

			p.mu.RLock()
			pub := p.publisher
			p.mu.RUnlock()
			if pub == nil {
				continue
			}

			if err := pub.Publish(event); err != nil {
				return err
			}
		}
	}
}

func (p *PipeConsumer) Name() string { return p.name }
func (c *PipeConsumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case out, ok := <-c.in:
			if !ok {
				return nil
			}
			select {
			case c.out <- out:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (c *PipeConsumer) Subscribe(topic string) {}

func (c *PipeConsumer) Unsubscribe(topic string) {}

func (c *PipeConsumer) GetChannel() <-chan dbspruntime.Event { return c.in }

func (c *PipeConsumer) Consume(ctx context.Context, out dbspruntime.Event) error {
	select {
	case c.out <- out:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
