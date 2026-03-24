package runtime

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
)

// ErrorReporter is the shared non-critical error reporting contract.
// Runtime implements this interface.
type ErrorReporter interface {
	ReportError(origin string, err error)
}

// ErrorReporterFunc adapts a function to ErrorReporter.
type ErrorReporterFunc func(origin string, err error)

// ReportError calls f(origin, err).
func (f ErrorReporterFunc) ReportError(origin string, err error) {
	f(origin, err)
}

// ConsumeHandler handles one runtime event.
type ConsumeHandler interface {
	Consume(ctx context.Context, event Event) error
}

// BaseComponent carries shared identity, logger, and error reporting.
type BaseComponent struct {
	ErrorReporter
	logr.Logger

	name string
}

func newBaseComponent(name string, reporter ErrorReporter, logger logr.Logger) (*BaseComponent, error) {
	if name == "" {
		return nil, fmt.Errorf("component name is required")
	}
	if reporter == nil {
		return nil, fmt.Errorf("error reporter is required")
	}

	return &BaseComponent{name: name, ErrorReporter: reporter, Logger: logger}, nil
}

// Name returns the component name.
func (c *BaseComponent) Name() string {
	return c.name
}

// HandleError reports err as a non-critical error from this component.
func (c *BaseComponent) HandleError(err error) {
	c.ErrorReporter.ReportError(c.name, err)
}

// BaseConsumerConfig configures a BaseConsumer.
type BaseConsumerConfig struct {
	Name string

	Subscriber
	ErrorReporter
	logr.Logger

	Topics []string
}

// BaseConsumer provides a reusable subscriber event loop.
type BaseConsumer struct {
	*BaseComponent
	Subscriber
	topics []string
}

// NewBaseConsumer creates a BaseConsumer and subscribes it to all configured topics.
func NewBaseConsumer(cfg BaseConsumerConfig) (*BaseConsumer, error) {
	if cfg.Subscriber == nil {
		return nil, fmt.Errorf("subscriber is required")
	}

	b, err := newBaseComponent(cfg.Name, cfg.ErrorReporter, cfg.Logger)
	if err != nil {
		return nil, err
	}

	c := &BaseConsumer{BaseComponent: b, Subscriber: cfg.Subscriber, topics: append([]string(nil), cfg.Topics...)}
	for _, topic := range cfg.Topics {
		c.Subscribe(topic)
	}

	return c, nil
}

// String implements fmt.Stringer.
func (c *BaseConsumer) String() string {
	return fmt.Sprintf("consumer<runtime>{name=%q, topics=%v}", c.Name(), c.topics)
}

// Run receives events from the embedded subscriber and calls h.Consume.
// Handler errors are non-critical and are reported through ErrorReporter.
func (c *BaseConsumer) Run(ctx context.Context, h ConsumeHandler) error {
	ch := c.GetChannel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case evt, ok := <-ch:
			if !ok {
				return nil
			}
			if err := h.Consume(ctx, evt); err != nil {
				c.HandleError(err)
			}
		}
	}
}

// BaseProducerConfig configures a BaseProducer.
type BaseProducerConfig struct {
	Name string

	Publisher
	ErrorReporter
	logr.Logger

	Topics []string
}

// BaseProducer provides shared identity and error reporting for producers.
type BaseProducer struct {
	*BaseComponent
	Publisher
	topics []string
}

// NewBaseProducer creates a BaseProducer.
func NewBaseProducer(cfg BaseProducerConfig) (*BaseProducer, error) {
	if cfg.Publisher == nil {
		return nil, fmt.Errorf("publisher is required")
	}

	b, err := newBaseComponent(cfg.Name, cfg.ErrorReporter, cfg.Logger)
	if err != nil {
		return nil, err
	}

	return &BaseProducer{BaseComponent: b, Publisher: cfg.Publisher, topics: append([]string(nil), cfg.Topics...)}, nil
}

// String implements fmt.Stringer.
func (p *BaseProducer) String() string {
	return fmt.Sprintf("producer<runtime>{name=%q, topics=%v}", p.Name(), p.topics)
}

// BaseProcessorConfig configures a BaseProcessor.
type BaseProcessorConfig struct {
	Name string

	Publisher
	Subscriber
	ErrorReporter
	logr.Logger

	Topics []string
}

// BaseProcessor provides a reusable processor event loop.
type BaseProcessor struct {
	*BaseComponent
	Publisher
	Subscriber
	topics []string
}

// NewBaseProcessor creates a BaseProcessor and subscribes it to all configured topics.
func NewBaseProcessor(cfg BaseProcessorConfig) (*BaseProcessor, error) {
	if cfg.Publisher == nil {
		return nil, fmt.Errorf("publisher is required")
	}
	if cfg.Subscriber == nil {
		return nil, fmt.Errorf("subscriber is required")
	}

	b, err := newBaseComponent(cfg.Name, cfg.ErrorReporter, cfg.Logger)
	if err != nil {
		return nil, err
	}

	p := &BaseProcessor{BaseComponent: b, Publisher: cfg.Publisher, Subscriber: cfg.Subscriber, topics: append([]string(nil), cfg.Topics...)}
	for _, topic := range cfg.Topics {
		p.Subscribe(topic)
	}

	return p, nil
}

// String implements fmt.Stringer.
func (p *BaseProcessor) String() string {
	return fmt.Sprintf("processor<runtime>{name=%q, topics=%v}", p.Name(), p.topics)
}

// Run receives events from the embedded subscriber and calls h.Consume.
// Handler errors are non-critical and are reported through ErrorReporter.
func (p *BaseProcessor) Run(ctx context.Context, h ConsumeHandler) error {
	ch := p.GetChannel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case evt, ok := <-ch:
			if !ok {
				return nil
			}
			if err := h.Consume(ctx, evt); err != nil {
				p.HandleError(err)
			}
		}
	}
}
