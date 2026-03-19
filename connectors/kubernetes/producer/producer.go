package producer

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crevent "sigs.k8s.io/controller-runtime/pkg/event"
	crpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"

	kpredicate "github.com/l7mp/connectors/kubernetes/runtime/predicate"
	dbunstructured "github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
)

// Config configures a Kubernetes watch-backed DBSP producer.
type Config struct {
	Client client.WithWatch
	List   client.ObjectList

	InputName     string
	Namespace     string
	LabelSelector *v1.LabelSelector
	Predicate     *kpredicate.Predicate

	Logger logr.Logger
}

// Producer watches Kubernetes objects and emits DBSP runtime inputs.
type Producer struct {
	client    client.WithWatch
	list      client.ObjectList
	inputName string

	listOpts   []client.ListOption
	predicates []crpredicate.TypedPredicate[client.Object]

	mu      sync.RWMutex
	handler dbspruntime.InputHandler
	state   map[string]*unstructured.Unstructured

	log logr.Logger
}

var _ dbspruntime.Producer = (*Producer)(nil)

// New creates a Kubernetes producer.
func New(cfg Config) (*Producer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("producer: nil client")
	}
	if cfg.List == nil {
		return nil, fmt.Errorf("producer: nil list")
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	inputName := cfg.InputName
	if inputName == "" {
		inputName = cfg.List.GetObjectKind().GroupVersionKind().Kind
		inputName = strings.TrimSuffix(inputName, "List")
		if inputName == "" {
			inputName = "input"
		}
	}

	p := &Producer{
		client:    cfg.Client,
		list:      cfg.List,
		inputName: inputName,
		state:     map[string]*unstructured.Unstructured{},
		log:       log.WithName("kubernetes-producer").WithValues("input", inputName),
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

// SetInputHandler sets the runtime input callback.
func (p *Producer) SetInputHandler(h dbspruntime.InputHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handler = h
}

// Start starts the watch loop.
func (p *Producer) Start(ctx context.Context) error {
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

func (p *Producer) handleEvent(ctx context.Context, evt watch.Event) error {
	obj, ok := evt.Object.(*unstructured.Unstructured)
	if !ok || obj == nil {
		return nil
	}

	key := objectKey(obj)
	var old *unstructured.Unstructured
	if cached, found := p.state[key]; found && cached != nil {
		old = cached.DeepCopy()
	}

	if !p.allow(evt.Type, old, obj) {
		if evt.Type == watch.Deleted {
			delete(p.state, key)
		} else {
			p.state[key] = obj.DeepCopy()
		}
		return nil
	}

	zs := zset.New()
	newDoc := toDocument(obj)

	switch evt.Type {
	case watch.Added:
		zs.Insert(newDoc, 1)
		p.state[key] = obj.DeepCopy()
	case watch.Modified:
		if old != nil {
			zs.Insert(toDocument(old), -1)
		}
		zs.Insert(newDoc, 1)
		p.state[key] = obj.DeepCopy()
	case watch.Deleted:
		if old != nil {
			zs.Insert(toDocument(old), -1)
		} else {
			zs.Insert(newDoc, -1)
		}
		delete(p.state, key)
	default:
		return nil
	}

	if zs.IsZero() {
		return nil
	}

	p.mu.RLock()
	h := p.handler
	p.mu.RUnlock()
	if h == nil {
		return nil
	}

	return h(ctx, dbspruntime.Input{Name: p.inputName, Data: zs})
}

func (p *Producer) allow(t watch.EventType, oldObj, newObj *unstructured.Unstructured) bool {
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
	gvk := obj.GroupVersionKind().String()
	return fmt.Sprintf("%s/%s/%s", gvk, obj.GetNamespace(), obj.GetName())
}

func toDocument(obj *unstructured.Unstructured) *dbunstructured.Unstructured {
	content := runtime.DeepCopyJSON(obj.UnstructuredContent())
	return dbunstructured.New(content, nil)
}
