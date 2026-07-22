package consumer

import (
	"context"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dbsp/engine/datamodel"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// Setter applies output events with state-of-the-world semantics: each event
// carries the complete desired state of the managed scope, and the Setter
// reconciles the cluster to it against a fresh List. Objects in the event
// are created or updated (an update is skipped when the current content
// already matches), and objects in scope but absent from the event are
// deleted.
//
// The Setter owns the entire target GVK: every object of the kind not
// present in the event is deleted. Placement (namespaces, labels) is the
// pipeline's business alone; a consumer-side scope filter would let the
// pipeline emit objects outside the managed scope, created by the Setter
// but invisible to its List and therefore never deleted. Point the Setter
// only at kinds whose full population the controller means to own.
type Setter struct {
	*baseConsumer
}

var _ dbspruntime.Consumer = (*Setter)(nil)

// NewSetter creates a state-of-the-world consumer.
func NewSetter(cfg Config) (*Setter, error) {
	b, err := newBase(cfg, "kubernetes-consumer-setter")
	if err != nil {
		return nil, err
	}
	return &Setter{baseConsumer: b}, nil
}

// Start runs the consumer event loop.
func (c *Setter) Start(ctx context.Context) error {
	return c.start(ctx, c)
}

// String implements fmt.Stringer.
func (c *Setter) String() string {
	return fmt.Sprintf("consumer<k8s-setter>{name=%q, topic=%q}", c.Name(), c.outputName)
}

// Consume reconciles the managed scope to the event's full state.
func (c *Setter) Consume(ctx context.Context, out dbspruntime.Event) error {
	desired := map[client.ObjectKey]*unstructured.Unstructured{}
	var convErr error
	out.Data.Iter(func(doc datamodel.Document, w zset.Weight) bool {
		if w <= 0 {
			convErr = fmt.Errorf("setter %s: level event on topic %q carries weight %d for %s; a full state has no retractions",
				c.Name(), out.Name, w, doc.String())
			return false
		}
		obj, err := toObject(doc)
		if err != nil {
			convErr = fmt.Errorf("setter %s: %w", c.Name(), err)
			return false
		}
		obj.SetGroupVersionKind(c.targetGVK)
		if obj.GetName() == "" {
			convErr = fmt.Errorf("setter %s: object without metadata.name in level event on topic %q", c.Name(), out.Name)
			return false
		}
		desired[client.ObjectKeyFromObject(obj)] = obj
		return true
	})
	if convErr != nil {
		return convErr
	}

	current, err := c.listScope(ctx)
	if err != nil {
		return err
	}

	for key, obj := range desired {
		if cur, ok := current[key]; ok && contentMatches(cur, obj) {
			continue
		}
		if err := c.upsert(ctx, obj); err != nil {
			return err
		}
	}
	for key, cur := range current {
		if _, ok := desired[key]; ok {
			continue
		}
		if err := c.client.Delete(ctx, cur); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("setter %s: delete %s: %w", c.Name(), key.String(), err)
		}
	}
	return nil
}

// listScope returns the current objects of the target GVK, keyed by
// namespace/name.
func (c *Setter) listScope(ctx context.Context) (map[client.ObjectKey]*unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	gvk := c.targetGVK
	gvk.Kind += "List"
	list.SetGroupVersionKind(gvk)

	if err := c.client.List(ctx, list); err != nil {
		return nil, fmt.Errorf("setter %s: list scope: %w", c.Name(), err)
	}

	out := make(map[client.ObjectKey]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		obj := &list.Items[i]
		out[client.ObjectKeyFromObject(obj)] = obj
	}
	return out, nil
}

// contentMatches reports whether writing desired over current would change
// nothing. The pipeline output is wholesale (status included: the upsert
// writes the status subresource whenever the desired object carries one),
// so the comparison is the full content with only the server-owned
// metadata fields stripped. A conservative false only costs a redundant
// update.
func contentMatches(current, desired *unstructured.Unstructured) bool {
	return reflect.DeepEqual(normalizedContent(current), normalizedContent(desired))
}

func normalizedContent(obj *unstructured.Unstructured) map[string]any {
	content := runtime.DeepCopyJSON(obj.UnstructuredContent())
	delete(content, "apiVersion")
	delete(content, "kind")
	if meta, ok := content["metadata"].(map[string]any); ok {
		for _, f := range []string{"resourceVersion", "uid", "generation", "creationTimestamp", "managedFields", "selfLink"} {
			delete(meta, f)
		}
		if len(meta) == 0 {
			delete(content, "metadata")
		}
	}
	return content
}
