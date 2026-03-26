package misc

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// PipeProducer forwards runtime inputs from a channel to an input handler.
type PipeProducer struct {
	*dbspruntime.BaseProducer

	in <-chan dbspruntime.Event
}

// PipeConsumer forwards runtime outputs to a channel.
type PipeConsumer struct {
	*dbspruntime.BaseConsumer

	out chan<- dbspruntime.Event
	in  chan dbspruntime.Event
}

type pipeSubscriber struct {
	ch chan dbspruntime.Event
}

func (s *pipeSubscriber) Subscribe(string) {}

func (s *pipeSubscriber) Unsubscribe(string) {}

func (s *pipeSubscriber) GetChannel() <-chan dbspruntime.Event { return s.ch }

var _ dbspruntime.Producer = (*PipeProducer)(nil)
var _ dbspruntime.Consumer = (*PipeConsumer)(nil)

func NewPipeProducer(name string, in <-chan dbspruntime.Event) *PipeProducer {
	if name == "" {
		name = "pipe-producer"
	}

	base, err := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          name,
		Publisher:     dbspruntime.PublishFunc(func(dbspruntime.Event) error { return nil }),
		ErrorReporter: dbspruntime.ErrorReporterFunc(func(string, error) {}),
		Logger:        logr.Discard(),
	})
	if err != nil {
		panic(err)
	}

	return &PipeProducer{BaseProducer: base, in: in}
}

func NewPipeConsumer(name string, out chan<- dbspruntime.Event) *PipeConsumer {
	if name == "" {
		name = "pipe-consumer"
	}

	in := make(chan dbspruntime.Event, dbspruntime.EventBufferSize)
	sub := &pipeSubscriber{ch: in}

	base, err := dbspruntime.NewBaseConsumer(dbspruntime.BaseConsumerConfig{
		Name:          name,
		Subscriber:    sub,
		ErrorReporter: dbspruntime.ErrorReporterFunc(func(string, error) {}),
		Logger:        logr.Discard(),
	})
	if err != nil {
		panic(err)
	}

	return &PipeConsumer{BaseConsumer: base, out: out, in: in}
}

func (p *PipeProducer) Name() string { return p.BaseProducer.Name() }

func (p *PipeProducer) String() string {
	if p == nil {
		return "producer<pipe>{<nil>}"
	}
	return fmt.Sprintf("producer<pipe>{name=%q, topic=%q}", p.Name(), "<passthrough>")
}

func (p *PipeProducer) SetPublisher(pub dbspruntime.Publisher) {
	if pub == nil {
		pub = dbspruntime.PublishFunc(func(dbspruntime.Event) error { return nil })
	}
	p.Publisher = pub
}

func (p *PipeProducer) Publish(event dbspruntime.Event) error {
	return p.BaseProducer.Publish(event)
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

			if err := p.Publish(event); err != nil {
				return err
			}
		}
	}
}

func (p *PipeConsumer) Name() string { return p.BaseConsumer.Name() }

func (p *PipeConsumer) String() string {
	if p == nil {
		return "consumer<pipe>{<nil>}"
	}
	return fmt.Sprintf("consumer<pipe>{name=%q, topic=%q}", p.Name(), "<passthrough>")
}

func (c *PipeConsumer) Start(ctx context.Context) error {
	return c.Run(ctx, c)
}

func (c *PipeConsumer) Consume(ctx context.Context, out dbspruntime.Event) error {
	select {
	case c.out <- out:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
