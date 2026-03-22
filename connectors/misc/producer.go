package misc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"

	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

const (
	OneShotSourceObjectName  = "one-shot-trigger"
	PeriodicSourceObjectName = "periodic-trigger"

	VirtualSourceTypeField      = "type"
	VirtualSourceKindField      = "kind"
	VirtualSourceNameField      = "name"
	VirtualSourceNamespaceField = "namespace"
	VirtualSourceTriggeredField = "triggeredAt"
)

type OneShotConfig struct {
	// Name is the unique component name used for error reporting. Required.
	Name      string
	InputName string
	// TriggerKind is copied into the emitted virtual source document's `kind`
	// field. If empty, InputName is used.
	TriggerKind string
	// Namespace, if set, is copied into the emitted virtual source document.
	Namespace string
	// TriggerName is copied into the emitted virtual source document's `name`
	// field. Defaults to OneShotSourceObjectName if empty.
	TriggerName string
	// Runtime is the engine runtime used to create a publisher.
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

type PeriodicConfig struct {
	// Name is the unique component name used for error reporting. Required.
	Name      string
	InputName string
	// TriggerKind is copied into the emitted virtual source document's `kind`
	// field. If empty, InputName is used.
	TriggerKind string
	// Namespace, if set, is copied into the emitted virtual source document.
	Namespace string
	// TriggerName is copied into the emitted virtual source document's `name`
	// field. Defaults to PeriodicSourceObjectName if empty.
	TriggerName string
	Period      time.Duration
	// Runtime is the engine runtime used to create a publisher.
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

type OneShotProducer struct {
	*baseProducer
}

type PeriodicProducer struct {
	*baseProducer
	period time.Duration
}

type baseProducer struct {
	name        string // component name (for error reporting)
	inputName   string
	sourceType  string
	triggerKind string
	namespace   string
	triggerName string

	mu        sync.RWMutex
	rt        *dbspruntime.Runtime
	publisher dbspruntime.Publisher
	log       logr.Logger
}

var _ dbspruntime.Producer = (*OneShotProducer)(nil)
var _ dbspruntime.Producer = (*PeriodicProducer)(nil)

func NewOneShotProducer(cfg OneShotConfig) (*OneShotProducer, error) {
	b, err := newBase(
		cfg.Runtime,
		cfg.Name,
		cfg.InputName,
		opv1a1OneShotSourceType,
		cfg.TriggerKind,
		cfg.Namespace,
		cfg.TriggerName,
		cfg.Logger,
		OneShotSourceObjectName,
		"one-shot-producer",
	)
	if err != nil {
		return nil, err
	}
	return &OneShotProducer{baseProducer: b}, nil
}

func NewPeriodicProducer(cfg PeriodicConfig) (*PeriodicProducer, error) {
	if cfg.Period <= 0 {
		return nil, fmt.Errorf("periodic producer requires positive period")
	}
	b, err := newBase(
		cfg.Runtime,
		cfg.Name,
		cfg.InputName,
		opv1a1PeriodicSourceType,
		cfg.TriggerKind,
		cfg.Namespace,
		cfg.TriggerName,
		cfg.Logger,
		PeriodicSourceObjectName,
		"periodic-producer",
	)
	if err != nil {
		return nil, err
	}
	return &PeriodicProducer{baseProducer: b, period: cfg.Period}, nil
}

// Name returns the producer's unique component name.
func (p *baseProducer) Name() string { return p.name }

// newBase constructs the shared producer state. Name uniqueness is enforced
// when the producer is passed to Runtime.Add.
func newBase(rt *dbspruntime.Runtime, name, input, sourceType, triggerKind, namespace, triggerName string, logger logr.Logger, defaultTriggerName, loggerName string) (*baseProducer, error) {
	if sourceType == "" {
		return nil, fmt.Errorf("source type is required")
	}
	if input == "" {
		input = triggerKind
	}
	if input == "" {
		return nil, fmt.Errorf("input name or trigger kind is required")
	}
	if triggerKind == "" {
		triggerKind = input
	}
	if triggerName == "" {
		triggerName = defaultTriggerName
	}
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}
	b := &baseProducer{
		name:        name,
		inputName:   input,
		sourceType:  sourceType,
		triggerKind: triggerKind,
		namespace:   namespace,
		triggerName: triggerName,
		rt:          rt,
		log:         logger.WithName(loggerName).WithValues("name", name, "input", input),
	}
	if rt != nil {
		b.publisher = rt.NewPublisher()
	}
	return b, nil
}

func (p *baseProducer) SetPublisher(pub dbspruntime.Publisher) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publisher = pub
}

func (p *baseProducer) Publish(event dbspruntime.Event) error {
	p.mu.RLock()
	pub := p.publisher
	p.mu.RUnlock()
	if pub == nil {
		return nil
	}
	return pub.Publish(event)
}

func (p *OneShotProducer) Start(ctx context.Context) error {
	if err := p.emit(); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (p *PeriodicProducer) Start(ctx context.Context) error {
	ticker := time.NewTicker(p.period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.emit(); err != nil {
				p.reportError(err)
			}
		}
	}
}

func (p *baseProducer) emit() error {
	zs := zset.New()
	zs.Insert(p.triggerDocument(), 1)

	p.mu.RLock()
	pub := p.publisher
	p.mu.RUnlock()
	if pub == nil {
		return nil
	}

	return pub.Publish(dbspruntime.Event{Name: p.inputName, Data: zs})
}

func (p *baseProducer) triggerDocument() *dbunstructured.Unstructured {
	fields := map[string]any{
		VirtualSourceTypeField:      p.sourceType,
		VirtualSourceKindField:      p.triggerKind,
		VirtualSourceNameField:      p.triggerName,
		VirtualSourceTriggeredField: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if p.namespace != "" {
		fields[VirtualSourceNamespaceField] = p.namespace
	}
	return dbunstructured.New(fields, nil)
}

func (p *baseProducer) reportError(err error) {
	if err == nil {
		return
	}
	if p.rt != nil {
		p.rt.ReportError(p.name, err)
		return
	}
	p.log.Error(err, "producer error")
}

const (
	opv1a1OneShotSourceType  = "OneShot"
	opv1a1PeriodicSourceType = "Periodic"
)
