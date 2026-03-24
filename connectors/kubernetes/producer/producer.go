package producer

import (
	"context"
	"fmt"
	"sort"

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
	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// Config configures a Kubernetes watch-backed DBSP producer.
type Config struct {
	Client    client.WithWatch
	SourceGVK schema.GroupVersionKind

	// Name is the unique component name used for error reporting. Required.
	Name          string
	InputName     string
	Namespace     string
	LabelSelector *v1.LabelSelector
	Predicate     *kpredicate.Predicate

	// Runtime is the engine runtime used to create a publisher.
	Runtime *dbspruntime.Runtime

	Logger logr.Logger
}

// Watcher watches Kubernetes objects and emits DBSP runtime inputs.
type Watcher struct {
	client    client.WithWatch
	list      client.ObjectList
	name      string
	inputName string

	listOpts   []client.ListOption
	predicates []crpredicate.TypedPredicate[client.Object]

	rt  *dbspruntime.Runtime
	pub dbspruntime.Publisher

	sourceCache map[schema.GroupVersionKind]*store.Store

	log logr.Logger
}

var _ dbspruntime.Producer = (*Watcher)(nil)

// Name returns the watcher's unique component name.
func (w *Watcher) Name() string { return w.name }

// String implements fmt.Stringer.
func (w *Watcher) String() string {
	return fmt.Sprintf("producer<k8s>{name=%q, topic=%q}", w.name, w.inputName)
}

// NewWatcher creates a Kubernetes producer. Name uniqueness is enforced when
// the watcher is passed to Runtime.Add.
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
		name:        cfg.Name,
		inputName:   inputName,
		rt:          cfg.Runtime,
		sourceCache: map[schema.GroupVersionKind]*store.Store{},
		log:         log.WithName("kubernetes-producer").WithValues("topic", inputName),
	}

	p.pub = cfg.Runtime.NewPublisher()

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

func (p *Watcher) Publish(event dbspruntime.Event) error {
	return p.pub.Publish(event)
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
				p.rt.ReportError(p.name, err)
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

	var docs []string
	if p.log.V(2).Enabled() {
		docs = k8sDocsSummary(zs)
	}
	dbspruntime.LogFlowEvent(p.log, "producer.emit", "producer", p.String(), "output", p.inputName, "", zs, docs, "watch_event", string(evt.Type))

	return p.pub.Publish(dbspruntime.Event{Name: p.inputName, Data: zs})
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

func k8sDocsSummary(zs zset.ZSet) []string {
	entries := zs.Entries()
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		u, ok := e.Document.(*dbunstructured.Unstructured)
		if !ok {
			out = append(out, fmt.Sprintf("%s@%d", e.Document.String(), e.Weight))
			continue
		}
		out = append(out, fmt.Sprintf("%s@%d", kobject.DumpContent(u.Fields()), e.Weight))
	}
	sort.Strings(out)
	return out
}
