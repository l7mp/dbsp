package producer

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crevent "sigs.k8s.io/controller-runtime/pkg/event"
	crpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	kpredicate "github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

type baseProducer struct {
	*dbspruntime.BaseProducer

	client    client.WithWatch
	sourceGVK schema.GroupVersionKind
	inputName string

	listOpts   []client.ListOption
	predicates []crpredicate.TypedPredicate[client.Object]

	sourceCache map[schema.GroupVersionKind]*store.Store

	converter kobject.Converter

	log logr.Logger
}

func newBase(cfg Config, producerType string) (*baseProducer, error) {
	if cfg.Runtime == nil {
		return nil, fmt.Errorf("producer: runtime is required")
	}
	if cfg.Client == nil {
		return nil, fmt.Errorf("producer: client is required")
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	inputName := cfg.InputName
	base, err := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          cfg.Name,
		Publisher:     cfg.Runtime.NewPublisher(),
		ErrorReporter: cfg.Runtime,
		Logger:        log.WithName(producerType).WithValues("topic", inputName),
		Topics:        []string{inputName},
	})
	if err != nil {
		return nil, err
	}

	converter := cfg.Converter
	if converter == nil {
		converter = kobject.DefaultConverter
	}

	p := &baseProducer{
		BaseProducer: base,
		client:       cfg.Client,
		sourceGVK:    cfg.SourceGVK,
		inputName:    inputName,
		sourceCache:  map[schema.GroupVersionKind]*store.Store{},
		converter:    converter,
		log:          log.WithName(producerType).WithValues("topic", inputName),
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

// start runs the watch loop. The API server closes watch connections
// routinely (request-timeout expiry, every 30-60 minutes by default), so a
// closed result channel is a normal event, not the end of the
// subscription: the loop re-establishes the watch from the last delivered
// resourceVersion (bookmarks are requested so idle watches track it too).
// A 410 Gone (the version fell out of the server's watch window) restarts
// from the current state with a loud component error, since deltas in the
// gap are lost until an upstream resync.
func (p *baseProducer) start(ctx context.Context, onEvent func(context.Context, watch.Event) error) error {
	first := true
	lastRV := ""
	backoff := wait.Backoff{Duration: time.Second, Factor: 2, Cap: 30 * time.Second, Steps: math.MaxInt32}

	for {
		opts := make([]client.ListOption, 0, len(p.listOpts)+1)
		opts = append(opts, p.listOpts...)
		opts = append(opts, &client.ListOptions{
			Raw: &v1.ListOptions{ResourceVersion: lastRV, AllowWatchBookmarks: true},
		})

		w, err := p.client.Watch(ctx, p.newListObject(), opts...)
		if err != nil {
			if first {
				return fmt.Errorf("producer: watch failed: %w", err)
			}
			if apierrors.IsGone(err) || apierrors.IsResourceExpired(err) {
				p.HandleError(fmt.Errorf("producer: watch resourceVersion %s expired, restarting from current state (deltas in the gap are lost): %w", lastRV, err))
				lastRV = ""
				continue
			}
			delay := backoff.Step()
			p.HandleError(fmt.Errorf("producer: watch reconnect failed, retrying in %s: %w", delay, err))

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(delay):
			}

			continue
		}
		first = false
		backoff = wait.Backoff{Duration: time.Second, Factor: 2, Cap: 30 * time.Second, Steps: math.MaxInt32}
		p.log.V(2).Info("watch started", "resourceVersion", lastRV)

		closed := false
		for !closed {
			select {
			case <-ctx.Done():
				w.Stop()
				return nil
			case evt, ok := <-w.ResultChan():
				if !ok {
					closed = true
					break
				}

				switch evt.Type { //nolint:exhaustive // Added/Modified/Deleted fall through to onEvent below.
				case watch.Bookmark:
					if obj, ok := evt.Object.(client.Object); ok {
						lastRV = obj.GetResourceVersion()
					}
					continue
				case watch.Error:
					werr := apierrors.FromObject(evt.Object)
					if apierrors.IsGone(werr) || apierrors.IsResourceExpired(werr) {
						p.HandleError(fmt.Errorf("producer: watch expired, restarting from current state (deltas in the gap are lost): %w", werr))
						lastRV = ""
					} else {
						p.HandleError(fmt.Errorf("producer: watch error: %w", werr))
					}
					closed = true
					continue
				}

				if obj, ok := evt.Object.(client.Object); ok {
					if rv := obj.GetResourceVersion(); rv != "" {
						lastRV = rv
					}
				}
				if err := onEvent(ctx, evt); err != nil {
					p.HandleError(err)
				}
			}
		}
		w.Stop()
		p.log.V(2).Info("watch closed, reconnecting", "resourceVersion", lastRV)
	}
}

func (p *baseProducer) allowEvent(t watch.EventType, oldObj, newObj *unstructured.Unstructured) bool {
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

func (p *baseProducer) allowObject(obj *unstructured.Unstructured) bool {
	if len(p.predicates) == 0 {
		return true
	}

	for _, pred := range p.predicates {
		if !pred.Create(crevent.TypedCreateEvent[client.Object]{Object: obj}) {
			return false
		}
	}

	return true
}

func (p *baseProducer) listSnapshot(ctx context.Context) (zset.ZSet, error) {
	list := p.newListObject()
	if err := p.client.List(ctx, list, p.listOpts...); err != nil {
		return zset.New(), fmt.Errorf("producer: list failed: %w", err)
	}

	zs := zset.New()
	for i := range list.Items {
		obj := list.Items[i].DeepCopy()
		if !p.allowObject(obj) {
			continue
		}
		zs.Insert(p.converter.ToDocument(obj), 1)
	}

	return zs, nil
}

func (p *baseProducer) newListObject() *unstructured.UnstructuredList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(p.sourceGVK)
	return list
}
