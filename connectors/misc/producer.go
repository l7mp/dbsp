package misc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

const (
	OneShotSourceObjectName  = "one-shot-trigger"
	PeriodicSourceObjectName = "periodic-trigger"
	VirtualSourceTriggered   = "dcontroller.io/last-triggered"
)

type OneShotConfig struct {
	// Name is the unique component name used for error reporting. Required.
	Name       string
	InputName  string
	TriggerGVK schema.GroupVersionKind
	Namespace  string
	// ObjectName is the Kubernetes object name of the trigger object. Defaults
	// to OneShotSourceObjectName if empty.
	ObjectName string
	// Runtime is the engine runtime used to create a publisher.
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

type PeriodicConfig struct {
	// Name is the unique component name used for error reporting. Required.
	Name       string
	InputName  string
	TriggerGVK schema.GroupVersionKind
	Namespace  string
	// ObjectName is the Kubernetes object name of the trigger object. Defaults
	// to PeriodicSourceObjectName if empty.
	ObjectName string
	Period     time.Duration
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
	name       string // component name (for error reporting)
	inputName  string
	gvk        schema.GroupVersionKind
	namespace  string
	objectName string // Kubernetes object name of the trigger object

	cache *store.Store

	mu        sync.RWMutex
	rt        *dbspruntime.Runtime
	publisher dbspruntime.Publisher
	log       logr.Logger
}

var _ dbspruntime.Producer = (*OneShotProducer)(nil)
var _ dbspruntime.Producer = (*PeriodicProducer)(nil)

func NewOneShotProducer(cfg OneShotConfig) (*OneShotProducer, error) {
	b, err := newBase(cfg.Runtime, cfg.Name, cfg.InputName, cfg.TriggerGVK, cfg.Namespace, cfg.ObjectName, cfg.Logger, OneShotSourceObjectName, "one-shot-producer")
	if err != nil {
		return nil, err
	}
	return &OneShotProducer{baseProducer: b}, nil
}

func NewPeriodicProducer(cfg PeriodicConfig) (*PeriodicProducer, error) {
	if cfg.Period <= 0 {
		return nil, fmt.Errorf("periodic producer requires positive period")
	}
	b, err := newBase(cfg.Runtime, cfg.Name, cfg.InputName, cfg.TriggerGVK, cfg.Namespace, cfg.ObjectName, cfg.Logger, PeriodicSourceObjectName, "periodic-producer")
	if err != nil {
		return nil, err
	}
	return &PeriodicProducer{baseProducer: b, period: cfg.Period}, nil
}

// Name returns the producer's unique component name.
func (p *baseProducer) Name() string { return p.name }

// newBase constructs the shared producer state. Name uniqueness is enforced
// when the producer is passed to Runtime.Add.
func newBase(rt *dbspruntime.Runtime, name, input string, gvk schema.GroupVersionKind, namespace, objectName string, logger logr.Logger, defaultObjectName, loggerName string) (*baseProducer, error) {
	if gvk.Kind == "" {
		return nil, fmt.Errorf("trigger GVK kind is required")
	}
	if input == "" {
		input = gvk.Kind
	}
	if objectName == "" {
		objectName = defaultObjectName
	}
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}
	b := &baseProducer{
		name:       name,
		inputName:  input,
		gvk:        gvk,
		namespace:  namespace,
		objectName: objectName,
		cache:      store.NewStore(),
		rt:         rt,
		log:        logger.WithName(loggerName).WithValues("name", name, "input", input),
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
	if err := p.emit(ctx, kobject.Added); err != nil {
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
			if err := p.emit(ctx, kobject.Updated); err != nil {
				p.rt.ReportError(p.name, err)
			}
		}
	}
}

func (p *baseProducer) emit(ctx context.Context, deltaType kobject.DeltaType) error {
	obj := p.triggerObject()
	zs, err := p.convertDeltaToZSet(kobject.Delta{Type: deltaType, Object: obj})
	if err != nil {
		return err
	}
	if zs.IsZero() {
		return nil
	}

	p.mu.RLock()
	pub := p.publisher
	p.mu.RUnlock()
	if pub == nil {
		return nil
	}

	return pub.Publish(dbspruntime.Event{Name: p.inputName, Data: zs})
}

func (p *baseProducer) triggerObject() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(p.gvk)
	obj.SetNamespace(p.namespace)
	obj.SetName(p.objectName)
	obj.SetLabels(map[string]string{VirtualSourceTriggered: time.Now().String()})
	return obj
}

func (p *baseProducer) convertDeltaToZSet(delta kobject.Delta) (zset.ZSet, error) {
	deltaObj := kobject.DeepCopy(delta.Object)

	var old kobject.Object
	if obj, exists, err := p.cache.Get(deltaObj); err == nil && exists {
		old = obj
	}

	kobject.RemoveUID(deltaObj)

	if old != nil && (delta.Type == kobject.Updated || delta.Type == kobject.Replaced || delta.Type == kobject.Upserted) {
		oldNoUID := kobject.DeepCopy(old)
		kobject.RemoveUID(oldNoUID)
		if kobject.DeepEqual(deltaObj, oldNoUID) {
			return zset.New(), nil
		}
	}

	zs := zset.New()
	switch delta.Type {
	case kobject.Added:
		zs.Insert(toDocument(deltaObj), 1)
		if err := p.cache.Add(deltaObj); err != nil {
			return zset.New(), err
		}
	case kobject.Updated, kobject.Replaced, kobject.Upserted:
		if old != nil {
			zs.Insert(toDocument(old), -1)
		}
		zs.Insert(toDocument(deltaObj), 1)
		if err := p.cache.Update(deltaObj); err != nil {
			return zset.New(), err
		}
	case kobject.Deleted:
		if old == nil {
			return zset.New(), fmt.Errorf("delete for non-existent object %s", client.ObjectKeyFromObject(deltaObj).String())
		}
		zs.Insert(toDocument(deltaObj), -1)
		if err := p.cache.Delete(old); err != nil {
			return zset.New(), err
		}
	default:
		return zset.New(), fmt.Errorf("unknown delta type %q", delta.Type)
	}

	return zs, nil
}

func toDocument(obj kobject.Object) *dbunstructured.Unstructured {
	return dbunstructured.New(obj.UnstructuredContent(), nil)
}
