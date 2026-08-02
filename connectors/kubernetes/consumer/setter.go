package consumer

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dbsp/engine/datamodel"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// Setter applies output events with state-of-the-world semantics: each event
// carries the complete desired state of the managed scope, and the Setter
// reconciles the cluster to it against a fresh List. Objects in the event
// are created or patched with the merge diff against the listed current
// object (an empty diff writes nothing), and objects in scope but absent
// from the event are deleted.
type Setter struct {
	*baseConsumer

	// lastDesired is the most recent level, kept for the unreachable-plant
	// retry: a level consumer gets no further event unless the desired
	// state changes, so the retry must re-run the reconcile itself.
	lastDesired map[client.ObjectKey]*unstructured.Unstructured
}

var _ dbspruntime.Consumer = (*Setter)(nil)

// NewSetter creates a state-of-the-world consumer.
func NewSetter(cfg Config) (*Setter, error) {
	b, err := newBase(cfg, "kubernetes-consumer-setter")
	if err != nil {
		return nil, err
	}
	b.owns = true
	s := &Setter{baseConsumer: b}
	b.retryFlush = s.reconcileLocked
	return s, nil
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
		obj, err := c.converter.ToObject(doc)
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

	c.writeM.Lock()
	defer c.writeM.Unlock()
	c.lastDesired = desired
	return c.reconcileLocked(ctx)
}

// reconcileLocked drives the cluster to the last received level. Refused
// writes are reported and dropped; unreachable writes arm the retry timer,
// which re-runs the reconcile from the stored level.
func (c *Setter) reconcileLocked(ctx context.Context) error {
	current, err := c.listScope(ctx)
	if err != nil {
		if classifyWriteError(err) == Unreachable {
			c.armRetryLocked(0)
		}
		return err
	}

	var errs []error
	unreachable := false
	for key, obj := range c.lastDesired {
		var outcome ApplyResult
		var err error
		if cur, ok := current[key]; ok {
			outcome, err = c.applyUpdate(ctx, cur, obj, true)
		} else {
			outcome, err = c.applyCreate(ctx, obj, true)
		}
		switch outcome {
		case Applied:
		case Refused:
			errs = append(errs, err)
		case Unreachable:
			unreachable = true
			c.log.V(1).Info("plant unreachable, reconcile pending", "object", key.String(), "error", err.Error())
		}
	}
	for key, cur := range current {
		if _, ok := c.lastDesired[key]; ok {
			continue
		}
		if err := c.client.Delete(ctx, cur); err != nil && !apierrors.IsNotFound(err) {
			if classifyWriteError(err) == Unreachable {
				unreachable = true
				continue
			}
			errs = append(errs, fmt.Errorf("setter %s: delete %s: %w", c.Name(), key.String(), err))
		}
	}

	if unreachable {
		c.armRetryLocked(0)
	} else {
		c.backoff = retryBackoff
	}
	return errors.Join(errs...)
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
