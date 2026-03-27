package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"
	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	kpredicate "github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
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
	*baseProducer
}

// Lister watches a source and emits full list snapshots on each watch event.
type Lister struct {
	*baseProducer
}

var _ dbspruntime.Producer = (*Watcher)(nil)
var _ dbspruntime.Producer = (*Lister)(nil)

// Name returns the watcher's unique component name.
func (w *Watcher) Name() string { return w.BaseProducer.Name() }

// Name returns the lister's unique component name.
func (l *Lister) Name() string { return l.BaseProducer.Name() }

// String implements fmt.Stringer.
func (w *Watcher) String() string {
	return fmt.Sprintf("producer<k8s-watcher>{name=%q, topic=%q}", w.Name(), w.inputName)
}

// String implements fmt.Stringer.
func (l *Lister) String() string {
	return fmt.Sprintf("producer<k8s-lister>{name=%q, topic=%q}", l.Name(), l.inputName)
}

// MarshalJSON provides a stable machine-readable representation.
func (w *Watcher) MarshalJSON() ([]byte, error) {
	if w == nil {
		return json.Marshal(map[string]any{"component": "producer", "type": "kubernetes", "nil": true})
	}

	return json.Marshal(map[string]any{
		"component": "producer",
		"type":      "kubernetes",
		"mode":      "watcher",
		"name":      w.Name(),
		"topic":     w.inputName,
	})
}

// MarshalJSON provides a stable machine-readable representation.
func (l *Lister) MarshalJSON() ([]byte, error) {
	if l == nil {
		return json.Marshal(map[string]any{"component": "producer", "type": "kubernetes", "nil": true})
	}

	return json.Marshal(map[string]any{
		"component": "producer",
		"type":      "kubernetes",
		"mode":      "lister",
		"name":      l.Name(),
		"topic":     l.inputName,
	})
}

// NewWatcher creates a Kubernetes producer. Name uniqueness is enforced when
// the watcher is passed to Runtime.Add.
func NewWatcher(cfg Config) (*Watcher, error) {
	b, err := newBase(cfg, "kubernetes-producer")
	if err != nil {
		return nil, err
	}

	return &Watcher{baseProducer: b}, nil
}

// NewLister creates a Kubernetes state-of-the-world producer. Name uniqueness
// is enforced when the lister is passed to Runtime.Add.
func NewLister(cfg Config) (*Lister, error) {
	b, err := newBase(cfg, "kubernetes-producer")
	if err != nil {
		return nil, err
	}

	return &Lister{baseProducer: b}, nil
}

func (p *Watcher) Publish(event dbspruntime.Event) error {
	return p.BaseProducer.Publish(event)
}

func (p *Lister) Publish(event dbspruntime.Event) error {
	return p.BaseProducer.Publish(event)
}

// Start starts the watch loop.
func (p *Watcher) Start(ctx context.Context) error {
	return p.baseProducer.start(ctx, p.handleEvent)
}

// Start starts the watch loop for list-triggered snapshots.
func (p *Lister) Start(ctx context.Context) error {
	return p.baseProducer.start(ctx, p.handleEvent)
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

	if !p.allowEvent(evt.Type, old, obj) {
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

	return p.Publish(dbspruntime.Event{Name: p.inputName, Data: zs})
}

func (p *Lister) handleEvent(ctx context.Context, evt watch.Event) error {
	obj, ok := evt.Object.(*unstructured.Unstructured)
	if !ok || obj == nil {
		return nil
	}

	zs, err := p.listSnapshot(ctx)
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

	return p.Publish(dbspruntime.Event{Name: p.inputName, Data: zs})
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

func objectKey(obj *unstructured.Unstructured) string {
	return client.ObjectKeyFromObject(obj).String()
}

func k8sDocsSummary(zs zset.ZSet) []string {
	entries := zs.Entries()
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		u, ok := e.Document.(*dbspunstructured.Unstructured)
		if !ok {
			out = append(out, fmt.Sprintf("%s@%d", e.Document.String(), e.Weight))
			continue
		}
		out = append(out, fmt.Sprintf("%s@%d", kobject.DumpContent(u.Fields()), e.Weight))
	}
	sort.Strings(out)
	return out
}
