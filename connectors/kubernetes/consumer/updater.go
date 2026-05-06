package consumer

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Updater applies output entries with update semantics.
type Updater struct {
	*baseConsumer
}

var _ dbspruntime.Consumer = (*Updater)(nil)

// NewUpdater creates an updater consumer.
func NewUpdater(cfg Config) (*Updater, error) {
	b, err := newBase(cfg, "kubernetes-consumer-updater")
	if err != nil {
		return nil, err
	}
	return &Updater{baseConsumer: b}, nil
}

// Start runs the consumer event loop, applying each received event with updater semantics.
func (c *Updater) Start(ctx context.Context) error {
	return c.start(ctx, c)
}

// Consume applies output Z-set deltas with updater behavior.
func (c *Updater) Consume(ctx context.Context, out dbspruntime.Event) error {
	deltas, err := c.classifyDeltas(out.Data)
	if err != nil {
		return err
	}

	for _, d := range deltas {
		pk := d.Key.String()
		dbspruntime.LogFlowApply(c.log, "consumer.apply", "consumer", c.String(), "apply", out.Name, "", pk, d.Weight, func() string {
			return kobject.DumpContent(d.Object.UnstructuredContent())
		})

		desired := d.Object
		if d.EventType == kobject.Deleted {
			if err := c.client.Delete(ctx, desired); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			continue
		}

		if err := c.upsert(ctx, desired); err != nil {
			return err
		}
	}

	return nil
}

// String implements fmt.Stringer.
func (c *Updater) String() string {
	return fmt.Sprintf("consumer<k8s-updater>{name=%q, topic=%q}", c.Name(), c.outputName)
}

func (c *Updater) upsert(ctx context.Context, desired *unstructured.Unstructured) error {
	key := client.ObjectKeyFromObject(desired)
	mainContent := runtime.DeepCopyJSON(desired.UnstructuredContent())
	statusValue, hasStatus := mainContent["status"]
	if hasStatus {
		delete(mainContent, "status")
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(desired.GroupVersionKind())
		current.SetName(desired.GetName())
		current.SetNamespace(desired.GetNamespace())

		err := c.client.Get(ctx, key, current)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}

			obj := &unstructured.Unstructured{}
			obj.SetUnstructuredContent(runtime.DeepCopyJSON(mainContent))
			obj.SetGroupVersionKind(desired.GroupVersionKind())
			obj.SetName(desired.GetName())
			obj.SetNamespace(desired.GetNamespace())
			return c.client.Create(ctx, obj)
		}

		obj := &unstructured.Unstructured{}
		obj.SetUnstructuredContent(runtime.DeepCopyJSON(mainContent))
		obj.SetGroupVersionKind(desired.GroupVersionKind())
		obj.SetName(desired.GetName())
		obj.SetNamespace(desired.GetNamespace())
		obj.SetResourceVersion(current.GetResourceVersion())
		return c.client.Update(ctx, obj)
	})
	if err != nil {
		return fmt.Errorf("consumer updater %s: %w", key.String(), err)
	}

	if hasStatus && !isViewObject(desired) {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &unstructured.Unstructured{}
			latest.SetGroupVersionKind(desired.GroupVersionKind())
			latest.SetName(desired.GetName())
			latest.SetNamespace(desired.GetNamespace())
			if err := c.client.Get(ctx, key, latest); err != nil {
				return err
			}

			statusObj := &unstructured.Unstructured{}
			statusObj.SetUnstructuredContent(map[string]any{})
			statusObj.SetGroupVersionKind(desired.GroupVersionKind())
			statusObj.SetName(desired.GetName())
			statusObj.SetNamespace(desired.GetNamespace())
			statusObj.SetResourceVersion(latest.GetResourceVersion())
			statusObj.Object["status"] = runtime.DeepCopyJSONValue(statusValue)

			return c.client.Status().Update(ctx, statusObj)
		}); err != nil {
			return fmt.Errorf("consumer updater %s status: %w", key.String(), err)
		}
	}

	return nil
}
