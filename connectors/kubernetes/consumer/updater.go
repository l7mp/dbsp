package consumer

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(desired.GroupVersionKind())
	obj.SetName(desired.GetName())
	obj.SetNamespace(desired.GetNamespace())

	_, err := createOrUpdate(ctx, c.client, obj, func() error {
		for k := range obj.UnstructuredContent() {
			if k == "metadata" {
				continue
			}
			if _, ok, _ := unstructured.NestedFieldNoCopy(desired.UnstructuredContent(), k); !ok {
				unstructured.RemoveNestedField(obj.UnstructuredContent(), k)
			}
		}

		for k, v := range desired.UnstructuredContent() {
			if k == "metadata" {
				continue
			}
			if err := unstructured.SetNestedField(obj.UnstructuredContent(), v, k); err != nil {
				c.log.Error(err, "update field failed", "key", key.String(), "field", k)
			}
		}

		mergeMetadata(obj, desired)
		obj.SetGroupVersionKind(desired.GroupVersionKind())
		obj.SetName(desired.GetName())
		obj.SetNamespace(desired.GetNamespace())
		return nil
	})

	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("consumer updater %s: %w", key.String(), err)
	}

	return nil
}
