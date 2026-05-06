package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Patcher applies output entries with merge-patch semantics.
type Patcher struct {
	*baseConsumer
}

var _ dbspruntime.Consumer = (*Patcher)(nil)

// NewPatcher creates a patcher consumer.
func NewPatcher(cfg Config) (*Patcher, error) {
	b, err := newBase(cfg, "kubernetes-consumer-patcher")
	if err != nil {
		return nil, err
	}
	return &Patcher{baseConsumer: b}, nil
}

// Start runs the consumer event loop, applying each received event with patcher semantics.
func (c *Patcher) Start(ctx context.Context) error {
	return c.start(ctx, c)
}

// Consume applies output Z-set deltas with patcher behavior.
func (c *Patcher) Consume(ctx context.Context, out dbspruntime.Event) error {
	deltas, err := c.classifyDeltas(out.Data)
	if err != nil {
		return err
	}

	for _, d := range deltas {
		pk := d.Key.String()

		dbspruntime.LogFlowApply(c.log, "consumer.apply", "consumer", c.String(),
			"apply", out.Name, "", pk, d.Weight, func() string {
				return kobject.DumpContent(d.Object.UnstructuredContent())
			})

		desired := d.Object
		if d.EventType == kobject.Deleted {
			if err := c.patchDelete(ctx, desired); err != nil {
				return err
			}
			continue
		}

		if err := c.patchUpsert(ctx, desired); err != nil {
			return err
		}
	}

	return nil
}

// String implements fmt.Stringer.
func (c *Patcher) String() string {
	return fmt.Sprintf("patcher<k8s>{name=%q topic=%q}", c.Name(), c.outputName)
}

func (c *Patcher) patchUpsert(ctx context.Context, desired *unstructured.Unstructured) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(desired.GroupVersionKind())
	obj.SetName(desired.GetName())
	obj.SetNamespace(desired.GetNamespace())

	patch := runtime.DeepCopyJSON(desired.UnstructuredContent())
	statusPatch, hasStatus := patch["status"]
	if hasStatus {
		delete(patch, "status")
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("consumer patcher upsert %s: marshal patch: %w", client.ObjectKeyFromObject(desired).String(), err)
	}

	if err := c.client.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
		if apierrors.IsNotFound(err) {
			u, err := NewUpdater(Config{Client: c.client, OutputName: c.outputName, TargetGVK: c.targetGVK, Logger: c.log})
			if err != nil {
				return err
			}
			return u.upsert(ctx, desired)
		}
		return fmt.Errorf("consumer patcher upsert %s: %w", client.ObjectKeyFromObject(desired).String(), err)
	}

	if hasStatus && !isViewObject(obj) {
		statusPatchBytes, err := json.Marshal(map[string]any{"status": statusPatch})
		if err != nil {
			return fmt.Errorf("consumer patcher upsert %s: marshal status patch: %w", client.ObjectKeyFromObject(desired).String(), err)
		}

		if err := c.client.Status().Patch(ctx, obj, client.RawPatch(types.MergePatchType, statusPatchBytes)); err != nil {
			return fmt.Errorf("consumer patcher upsert %s status: %w", client.ObjectKeyFromObject(desired).String(), err)
		}
	}

	return nil
}

func (c *Patcher) patchDelete(ctx context.Context, desired *unstructured.Unstructured) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(desired.GroupVersionKind())
	obj.SetName(desired.GetName())
	obj.SetNamespace(desired.GetNamespace())

	patch := kobject.RemoveNestedMap(desired.UnstructuredContent())
	statusPatch, hasStatus := patch["status"]
	if hasStatus {
		delete(patch, "status")
	}

	gvk := desired.GroupVersionKind()
	unstructured.SetNestedField(patch, schema.GroupVersion{Group: gvk.Group, Version: gvk.Version}.String(), "apiVersion") //nolint:errcheck
	unstructured.SetNestedField(patch, gvk.Kind, "kind")                                                                   //nolint:errcheck
	unstructured.SetNestedField(patch, desired.GetNamespace(), "metadata", "namespace")                                    //nolint:errcheck
	unstructured.SetNestedField(patch, desired.GetName(), "metadata", "name")                                              //nolint:errcheck

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("consumer patcher delete %s: marshal patch: %w", client.ObjectKeyFromObject(desired).String(), err)
	}

	if err := c.client.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("consumer patcher delete %s: %w", client.ObjectKeyFromObject(desired).String(), err)
	}

	if hasStatus && !isViewObject(obj) {
		statusPatchBytes, err := json.Marshal(map[string]any{"status": statusPatch})
		if err != nil {
			return fmt.Errorf("consumer patcher delete %s: marshal status patch: %w", client.ObjectKeyFromObject(desired).String(), err)
		}

		if err := c.client.Status().Patch(ctx, obj, client.RawPatch(types.MergePatchType, statusPatchBytes)); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("consumer patcher delete %s status: %w", client.ObjectKeyFromObject(desired).String(), err)
		}
	}

	return nil
}
