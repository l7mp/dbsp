package consumer

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbspruntime "github.com/l7mp/dbsp/dbsp/runtime"
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

// Consume applies output Z-set deltas with updater behavior.
func (c *Updater) Consume(ctx context.Context, out dbspruntime.Output) error {
	if c.outputName != "" && out.Name != c.outputName {
		return nil
	}

	for _, e := range out.Data.Entries() {
		desired, isDelete, err := c.objectFromElem(e)
		if err != nil {
			return err
		}
		if desired == nil {
			continue
		}

		if isDelete {
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
