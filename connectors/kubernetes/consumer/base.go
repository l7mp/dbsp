package consumer

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/engine/datamodel"
	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// Config configures Kubernetes consumers.
type Config struct {
	Client client.Client

	// Name is the unique component name used for error reporting. Required.
	Name       string
	OutputName string
	TargetGVK  schema.GroupVersionKind

	// Runtime is the engine runtime used to create a subscriber.
	Runtime *dbspruntime.Runtime

	Logger logr.Logger
}

type baseConsumer struct {
	*dbspruntime.BaseConsumer

	client     client.Client
	outputName string
	targetGVK  schema.GroupVersionKind
	log        logr.Logger

	knownM sync.Mutex
	known  map[client.ObjectKey]struct{}
}

type classifiedDelta struct {
	EventType kobject.DeltaType
	Key       client.ObjectKey
	Object    kobject.Object
	Weight    zset.Weight
}

type candidate struct {
	obj    kobject.Object
	weight zset.Weight
}

type groupedDelta struct {
	key client.ObjectKey
	pos map[string]*candidate
	neg map[string]*candidate
}

// newBase constructs the shared consumer state. Name uniqueness is enforced
// when the consumer is passed to Runtime.Add.
func newBase(cfg Config, consumerType string) (*baseConsumer, error) {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName(consumerType).WithValues("topic", cfg.OutputName)

	if cfg.Runtime == nil {
		return nil, fmt.Errorf("runtime is required")
	}

	base, err := dbspruntime.NewBaseConsumer(dbspruntime.BaseConsumerConfig{
		Name:          cfg.Name,
		Subscriber:    cfg.Runtime.NewSubscriber(),
		ErrorReporter: cfg.Runtime,
		Logger:        log,
		Topics:        []string{cfg.OutputName},
	})
	if err != nil {
		return nil, err
	}

	b := &baseConsumer{
		BaseConsumer: base,
		client:       cfg.Client,
		outputName:   cfg.OutputName,
		targetGVK:    cfg.TargetGVK,
		log:          log,
		known:        map[client.ObjectKey]struct{}{},
	}

	return b, nil
}

// start is the shared event loop for Patcher and Updater.
// consume is called for every event received on the subscriber channel.
// Consume errors are non-critical: they are reported via the runtime error
// channel and the consumer continues processing subsequent events.
func (c *baseConsumer) start(ctx context.Context, consume dbspruntime.ConsumeHandler) error {
	return c.Run(ctx, consume)
}

func (c *baseConsumer) objectFromElem(e zset.Elem) (kobject.Object, bool, error) {
	obj, err := toObject(e.Document)
	if err != nil {
		return nil, false, err
	}

	obj = normalizeResultObject(obj, c.targetGVK)
	return obj, e.Weight < 0, nil
}

// classifyDeltas groups event entries by object key and derives one effective
// delta per key.
func (c *baseConsumer) classifyDeltas(data zset.ZSet) ([]classifiedDelta, error) {
	groups := map[string]*groupedDelta{}

	for _, e := range data.Entries() {
		obj, isDelete, err := c.objectFromElem(e)
		if err != nil {
			return nil, err
		}
		if obj == nil {
			continue
		}

		key := client.ObjectKeyFromObject(obj)
		groupKey := key.String()
		g, ok := groups[groupKey]
		if !ok {
			g = &groupedDelta{key: key, pos: map[string]*candidate{}, neg: map[string]*candidate{}}
			groups[groupKey] = g
		}

		docHash := e.Document.Hash()
		if isDelete {
			cand, ok := g.neg[docHash]
			if !ok {
				cand = &candidate{obj: obj}
				g.neg[docHash] = cand
			}
			cand.weight += e.Weight
			continue
		}

		cand, ok := g.pos[docHash]
		if !ok {
			cand = &candidate{obj: obj}
			g.pos[docHash] = cand
		}
		cand.weight += e.Weight
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]classifiedDelta, 0, len(keys))

	c.knownM.Lock()
	defer c.knownM.Unlock()

	for _, key := range keys {
		g := groups[key]
		cancelOppositeWeights(g.pos, g.neg)

		hasPos := hasResidualWeight(g.pos)
		hasNeg := hasResidualWeight(g.neg)
		if !hasPos && !hasNeg {
			continue
		}

		if hasPos && hasNeg {
			obj, w := selectCandidate(g.pos)
			if obj == nil {
				continue
			}
			c.known[g.key] = struct{}{}
			out = append(out, classifiedDelta{
				EventType: kobject.Updated,
				Key:       g.key,
				Object:    obj,
				Weight:    w,
			})
			continue
		}

		if hasPos {
			obj, w := selectCandidate(g.pos)
			if obj == nil {
				continue
			}
			eventType := kobject.Added
			if _, ok := c.known[g.key]; ok {
				eventType = kobject.Updated
			}
			c.known[g.key] = struct{}{}
			out = append(out, classifiedDelta{
				EventType: eventType,
				Key:       g.key,
				Object:    obj,
				Weight:    w,
			})
			continue
		}

		obj, w := selectCandidate(g.neg)
		if obj == nil {
			continue
		}
		delete(c.known, g.key)
		out = append(out, classifiedDelta{
			EventType: kobject.Deleted,
			Key:       g.key,
			Object:    obj,
			Weight:    w,
		})
	}

	return out, nil
}

func cancelOppositeWeights(pos, neg map[string]*candidate) {
	for hash, p := range pos {
		n, ok := neg[hash]
		if !ok {
			continue
		}

		pWeight := p.weight
		nWeight := -n.weight
		if pWeight <= 0 || nWeight <= 0 {
			continue
		}

		cancel := minWeight(pWeight, nWeight)
		p.weight -= cancel
		n.weight += cancel

		if p.weight == 0 {
			delete(pos, hash)
		}
		if n.weight == 0 {
			delete(neg, hash)
		}
	}
}

func hasResidualWeight(cands map[string]*candidate) bool {
	for _, c := range cands {
		if c.weight != 0 {
			return true
		}
	}
	return false
}

func selectCandidate(cands map[string]*candidate) (kobject.Object, zset.Weight) {
	hashes := make([]string, 0, len(cands))
	for hash, c := range cands {
		if c.weight != 0 {
			hashes = append(hashes, hash)
		}
	}
	if len(hashes) == 0 {
		return nil, 0
	}
	sort.Strings(hashes)
	chosen := cands[hashes[len(hashes)-1]]
	return chosen.obj, chosen.weight
}

func minWeight(a, b zset.Weight) zset.Weight {
	if a < b {
		return a
	}
	return b
}

func toObject(doc datamodel.Document) (kobject.Object, error) {
	udoc, ok := doc.(*dbunstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("consumer: unsupported document type %T", doc)
	}

	obj := kobject.New()
	obj.SetUnstructuredContent(udoc.Fields())

	return obj, nil
}

func normalizeResultObject(obj kobject.Object, target schema.GroupVersionKind) kobject.Object {
	doc := obj.UnstructuredContent()

	meta, ok := doc["metadata"]
	if !ok {
		return nil
	}
	metaMap, ok := meta.(map[string]any)
	if !ok {
		return nil
	}

	name, ok := metaMap["name"]
	if !ok {
		return nil
	}
	nameStr, ok := name.(string)
	if !ok || nameStr == "" {
		return nil
	}

	namespaceStr := ""
	if namespace, ok := metaMap["namespace"]; ok {
		nsStr, ok := namespace.(string)
		if !ok {
			return nil
		}
		namespaceStr = nsStr
	}

	ret := kobject.New()
	kobject.SetContent(ret, doc)
	ret.SetGroupVersionKind(target)
	ret.SetName(nameStr)
	ret.SetNamespace(namespaceStr)
	return ret
}

func isViewObject(obj client.Object) bool {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return viewv1a1.IsViewKind(gvk)
}

func createOrUpdate(ctx context.Context, c client.Client, obj kobject.Object, f controllerutil.MutateFn) (controllerutil.OperationResult, error) {
	key := client.ObjectKeyFromObject(obj)
	if err := c.Get(ctx, key, obj); err != nil {
		if !apierrors.IsNotFound(err) {
			return controllerutil.OperationResultNone, err
		}
		if err := mutate(f, key, obj); err != nil {
			return controllerutil.OperationResultNone, err
		}
		if err := c.Create(ctx, obj); err != nil {
			goto update
		}
		return controllerutil.OperationResultCreated, nil
	}

update:
	if err := mutate(f, key, obj); err != nil {
		return controllerutil.OperationResultNone, err
	}

	newStatus, hasStatus, _ := unstructured.NestedMap(obj.UnstructuredContent(), "status")
	if err := c.Update(ctx, obj); err != nil {
		return controllerutil.OperationResultNone, err
	}

	if hasStatus && !isViewObject(obj) {
		if err := unstructured.SetNestedMap(obj.UnstructuredContent(), newStatus, "status"); err == nil {
			if err := c.Status().Update(ctx, obj); err != nil {
				return controllerutil.OperationResultNone, err
			}
		}
	}

	return controllerutil.OperationResultUpdated, nil
}

func updateWithStatus(ctx context.Context, c client.Client, obj kobject.Object) error {
	savedStatus, hasStatus, _ := unstructured.NestedMap(obj.UnstructuredContent(), "status")
	key := client.ObjectKeyFromObject(obj)
	firstAttempt := true

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if !firstAttempt {
			latest := kobject.New()
			latest.SetGroupVersionKind(obj.GroupVersionKind())
			latest.SetName(key.Name)
			latest.SetNamespace(key.Namespace)
			if err := c.Get(ctx, key, latest); err != nil {
				return err
			}
			obj.SetResourceVersion(latest.GetResourceVersion())
		}
		firstAttempt = false

		if err := c.Update(ctx, obj); err != nil {
			return err
		}

		if hasStatus && !isViewObject(obj) {
			if err := unstructured.SetNestedMap(obj.UnstructuredContent(), savedStatus, "status"); err == nil {
				if err := c.Status().Update(ctx, obj); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func mutate(f controllerutil.MutateFn, key client.ObjectKey, obj client.Object) error {
	if err := f(); err != nil {
		return err
	}
	if newKey := client.ObjectKeyFromObject(obj); key != newKey {
		return fmt.Errorf("MutateFn cannot mutate object name and/or object namespace")
	}
	return nil
}

func mergeMetadata(obj, newObj kobject.Object) {
	labels := obj.GetLabels()
	newLabels := newObj.GetLabels()
	if newLabels != nil {
		if labels == nil {
			labels = map[string]string{}
		}
		for k, v := range newLabels {
			labels[k] = v
		}
		obj.SetLabels(labels)
	}

	annotations := obj.GetAnnotations()
	newAnnotations := newObj.GetAnnotations()
	if newAnnotations != nil {
		if annotations == nil {
			annotations = map[string]string{}
		}
		for k, v := range newAnnotations {
			annotations[k] = v
		}
		obj.SetAnnotations(annotations)
	}
}
