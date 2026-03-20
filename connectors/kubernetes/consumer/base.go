package consumer

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"

	"github.com/l7mp/dbsp/engine/datamodel"
	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
)

// Config configures Kubernetes consumers.
type Config struct {
	Client client.Client

	OutputName string
	TargetGVK  schema.GroupVersionKind

	Logger logr.Logger
}

type baseConsumer struct {
	client client.Client

	outputName string
	targetGVK  schema.GroupVersionKind

	log logr.Logger
	in  chan dbspruntime.Event
}

func newBase(cfg Config, name string) (*baseConsumer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("consumer: nil client")
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	return &baseConsumer{
		client:     cfg.Client,
		outputName: cfg.OutputName,
		targetGVK:  cfg.TargetGVK,
		log:        log.WithName(name).WithValues("output", cfg.OutputName),
		in:         make(chan dbspruntime.Event, dbspruntime.EventBufferSize),
	}, nil
}

func (c *baseConsumer) Subscribe(topic string) {}

func (c *baseConsumer) Unsubscribe(topic string) {}

func (c *baseConsumer) GetChannel() <-chan dbspruntime.Event { return c.in }

func (c *baseConsumer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.in:
		}
	}
}

func (c *baseConsumer) objectFromElem(e zset.Elem) (kobject.Object, bool, error) {
	obj, err := toObject(e.Document)
	if err != nil {
		return nil, false, err
	}

	obj = normalizeResultObject(obj, c.targetGVK)
	return obj, e.Weight < 0, nil
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
