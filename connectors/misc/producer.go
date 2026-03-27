package misc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
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
	*dbspruntime.BaseProducer

	inputName   string
	sourceType  string
	triggerKind string
	namespace   string
	triggerName string

	log logr.Logger
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
func (p *baseProducer) Name() string { return p.BaseProducer.Name() }

// String implements fmt.Stringer.
func (p *baseProducer) String() string {
	if p == nil {
		return "producer<misc>{<nil>}"
	}
	return fmt.Sprintf("producer<misc>{name=%q, topic=%q, type=%q}", p.Name(), p.inputName, p.sourceType)
}

// MarshalJSON provides a stable machine-readable representation.
func (p *baseProducer) MarshalJSON() ([]byte, error) {
	if p == nil {
		return json.Marshal(map[string]any{"component": "producer", "type": "misc", "nil": true})
	}

	return json.Marshal(map[string]any{
		"component":  "producer",
		"type":       "misc",
		"name":       p.Name(),
		"topic":      p.inputName,
		"sourceType": p.sourceType,
	})
}

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

	if name == "" {
		name = fmt.Sprintf("%s.%s", loggerName, input)
	}

	var publisher dbspruntime.Publisher = dbspruntime.PublishFunc(func(dbspruntime.Event) error { return nil })
	var reporter dbspruntime.ErrorReporter = dbspruntime.ErrorReporterFunc(func(_ string, err error) {
		if err == nil {
			return
		}
		logger.Error(err, "producer error")
	})
	if rt != nil {
		publisher = rt.NewPublisher()
		reporter = rt
	}

	base, err := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          name,
		Publisher:     publisher,
		ErrorReporter: reporter,
		Logger:        logger.WithName(loggerName).WithValues("topic", input),
		Topics:        []string{input},
	})
	if err != nil {
		return nil, err
	}

	b := &baseProducer{
		BaseProducer: base,
		inputName:    input,
		sourceType:   sourceType,
		triggerKind:  triggerKind,
		namespace:    namespace,
		triggerName:  triggerName,
		log:          logger.WithName(loggerName).WithValues("topic", input),
	}

	return b, nil
}

func (p *baseProducer) SetPublisher(pub dbspruntime.Publisher) {
	if pub == nil {
		pub = dbspruntime.PublishFunc(func(dbspruntime.Event) error { return nil })
	}
	p.Publisher = pub
}

func (p *baseProducer) Publish(event dbspruntime.Event) error {
	return p.BaseProducer.Publish(event)
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

	dbspruntime.LogFlowEvent(p.log, "producer.emit", "producer", p.String(), "output", p.inputName, "", zs, nil)

	return p.Publish(dbspruntime.Event{Name: p.inputName, Data: zs})
}

func (p *baseProducer) triggerDocument() *dbspunstructured.Unstructured {
	fields := map[string]any{
		VirtualSourceTypeField:      p.sourceType,
		VirtualSourceKindField:      p.triggerKind,
		VirtualSourceNameField:      p.triggerName,
		VirtualSourceTriggeredField: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if p.namespace != "" {
		fields[VirtualSourceNamespaceField] = p.namespace
	}
	return dbspunstructured.New(fields, nil)
}

func (p *baseProducer) reportError(err error) {
	if err == nil {
		return
	}
	p.HandleError(err)
}

const (
	opv1a1OneShotSourceType  = "OneShot"
	opv1a1PeriodicSourceType = "Periodic"
)
