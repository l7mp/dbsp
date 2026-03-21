package producer

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crevent "sigs.k8s.io/controller-runtime/pkg/event"
	crpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	kpredicate "github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Config configures a Kubernetes watch-backed DBSP producer.
type Config struct {
	Client    client.WithWatch
	SourceGVK schema.GroupVersionKind

	InputName     string
	Namespace     string
	LabelSelector *v1.LabelSelector
	Predicate     *kpredicate.Predicate

	// Runtime is the engine runtime used to create a publisher. If Runtime is set, a
	// publisher is automatically obtained from Runtime.NewPublisher(). SetPublisher() can be
	// used afterwards to override it.
	Runtime *dbspruntime.Runtime

	Logger logr.Logger
}

// Watcher watches Kubernetes objects and emits DBSP runtime inputs.
type Watcher struct {
	client    client.WithWatch
	list      client.ObjectList
	inputName string

	listOpts   []client.ListOption
	predicates []crpredicate.TypedPredicate[client.Object]

	mu  sync.RWMutex
	pub dbspruntime.Publisher

	sourceCache map[schema.GroupVersionKind]*store.Store

	log logr.Logger
}

var _ dbspruntime.Producer = (*Watcher)(nil)

// NewWatcher creates a Kubernetes producer.
func NewWatcher(cfg Config) (*Watcher, error) {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	inputName := cfg.InputName
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(cfg.SourceGVK)

	p := &Watcher{
		client:      cfg.Client,
		list:        list,
		inputName:   inputName,
		sourceCache: map[schema.GroupVersionKind]*store.Store{},
		log:         log.WithName("kubernetes-producer").WithValues("input", inputName),
	}

	if cfg.Runtime != nil {
		p.pub = cfg.Runtime.NewPublisher()
	}

	if cfg.Namespace != "" {
		p.listOpts = append(p.listOpts, client.InNamespace(cfg.Namespace))
	}

	if cfg.LabelSelector != nil {
		lp, err := kpredicate.FromLabelSelector(*cfg.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("producer: invalid label selector: %w", err)
		}
		p.predicates = append(p.predicates, lp)

		sel, err := v1.LabelSelectorAsSelector(cfg.LabelSelector)
		if err == nil {
			p.listOpts = append(p.listOpts, client.MatchingLabelsSelector{Selector: sel})
		}
	}

	if cfg.Predicate != nil {
		pred, err := kpredicate.FromPredicate(*cfg.Predicate)
		if err != nil {
			return nil, fmt.Errorf("producer: invalid predicate: %w", err)
		}
		p.predicates = append(p.predicates, pred)
	}

	if cfg.Namespace != "" {
		p.predicates = append(p.predicates, kpredicate.FromNamespace(cfg.Namespace))
	}

	return p, nil
}

// SetPublisher sets the runtime event publisher.
func (p *Watcher) SetPublisher(pub dbspruntime.Publisher) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pub = pub
}

func (p *Watcher) Publish(event dbspruntime.Event) error {
	p.mu.RLock()
	pub := p.pub
	p.mu.RUnlock()
	if pub == nil {
		return nil
	}
	return pub.Publish(event)
}

// Start starts the watch loop.
func (p *Watcher) Start(ctx context.Context) error {
	w, err := p.client.Watch(ctx, p.list, p.listOpts...)
	if err != nil {
		return fmt.Errorf("producer: watch failed: %w", err)
	}
	defer w.Stop()

	p.log.V(2).Info("watch started")

	for {
		select {
		case <-ctx.Done():
			return nil
		case evt, ok := <-w.ResultChan():
			if !ok {
				return nil
			}

			if err := p.handleEvent(ctx, evt); err != nil {
				return err
			}
		}
	}
}

func (p *Watcher) handleEvent(ctx context.Context, evt watch.Event) error {
	obj, ok := evt.Object.(*unstructured.Unstructured)
	if !ok || obj == nil {
		return nil
	}

	var old *unstructured.Unstructured
	gvk := obj.GroupVersionKind()
	if cache, ok := p.sourceCache[gvk]; ok {
		if cached, exists, _ := cache.Get(obj); exists && cached != nil {
			old = cached.DeepCopy()
		}
	}

	if !p.allow(evt.Type, old, obj) {
		return nil
	}

	delta := watchEventToDelta(evt.Type, obj)
	zs, err := p.convertDeltaToZSet(delta)
	if err != nil {
		return err
	}

	if zs.IsZero() {
		return nil
	}

	p.mu.RLock()
	pub := p.pub
	p.mu.RUnlock()
	if pub == nil {
		return nil
	}

	return pub.Publish(dbspruntime.Event{Name: p.inputName, Data: zs})
}

func watchEventToDelta(t watch.EventType, obj *unstructured.Unstructured) kobject.Delta {
	switch t {
	case watch.Added:
		return kobject.Delta{Type: kobject.Added, Object: obj}
	case watch.Modified:
		return kobject.Delta{Type: kobject.Updated, Object: obj}
	case watch.Deleted:
		return kobject.Delta{Type: kobject.Deleted, Object: obj}
	default:
		return kobject.NilDelta
	}
}

func (p *Watcher) allow(t watch.EventType, oldObj, newObj *unstructured.Unstructured) bool {
	if len(p.predicates) == 0 {
		return true
	}

	for _, pred := range p.predicates {
		var ok bool
		switch t {
		case watch.Added:
			ok = pred.Create(crevent.TypedCreateEvent[client.Object]{Object: newObj})
		case watch.Modified:
			if oldObj == nil {
				ok = true
			} else {
				ok = pred.Update(crevent.TypedUpdateEvent[client.Object]{ObjectOld: oldObj, ObjectNew: newObj})
			}
		case watch.Deleted:
			ok = pred.Delete(crevent.TypedDeleteEvent[client.Object]{Object: newObj})
		default:
			ok = false
		}
		if !ok {
			return false
		}
	}

	return true
}

func objectKey(obj *unstructured.Unstructured) string {
	return client.ObjectKeyFromObject(obj).String()
}
