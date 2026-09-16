package consumer

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/engine/datamodel"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// The Kubernetes half of the write path: the data model on one side, the
// apiserver's command set on the other. The shared half (pairing, the
// outstanding-job queue, the emitter) lives in engine/runtime.

// Adapt converts a pipeline document into the object a write addresses.
// The document must name an object: without metadata.name there is no
// plant key and no command can be formed, so an unaddressable document is
// reported rather than dropped. The consumer's target GVK is stamped onto
// the result, which is what turns a pipeline document (data, no
// addressing) into a writable object.
func (c *baseConsumer) Adapt(doc datamodel.Document) (string, any, error) {
	obj, err := c.converter.ToObject(doc)
	if err != nil {
		return "", nil, err
	}

	content := obj.UnstructuredContent()
	meta, ok := content["metadata"].(map[string]any)
	if !ok {
		return "", nil, fmt.Errorf("document carries no metadata")
	}
	name, ok := meta["name"].(string)
	if !ok || name == "" {
		return "", nil, fmt.Errorf("document carries no object name")
	}
	namespace := ""
	if ns, exists := meta["namespace"]; exists {
		if namespace, ok = ns.(string); !ok {
			return "", nil, fmt.Errorf("object namespace is not a string")
		}
	}

	target := kobject.New()
	kobject.SetContent(target, content)
	target.SetGroupVersionKind(c.targetGVK)
	target.SetName(name)
	target.SetNamespace(namespace)

	return client.ObjectKeyFromObject(target).String(), target, nil
}

// Equal reports whether two objects leave the plant in the same state.
func (c *baseConsumer) Equal(a, b any) bool {
	return kobject.DeepEqual(a.(kobject.Object), b.(kobject.Object))
}

// Apply writes one outstanding change. The pair shape picks the edge: a
// full pair is an update, a bare assertion is a create (owner) or a
// first-decoration patch, a bare retraction is a delete (owner) or a
// field-clearing patch.
func (c *baseConsumer) Apply(ctx context.Context, key string, old, new any) (dbspruntime.ApplyResult, error) {
	oldObj, _ := old.(kobject.Object)
	newObj, _ := new.(kobject.Object)

	mode, weight, target := "update", zset.Weight(1), newObj
	switch {
	case newObj == nil:
		mode, weight, target = "retract", -1, oldObj
	case oldObj == nil:
		mode = "assert"
	}
	dbspruntime.LogFlowApply(c.log, "consumer.apply", "consumer", c.String(),
		"apply", c.outputName, "", key, weight, func() string {
			return kobject.DumpContent(target.UnstructuredContent())
		}, "mode", mode)

	var outcome dbspruntime.ApplyResult
	var err error
	switch {
	case newObj == nil:
		outcome, err = c.applyRetract(ctx, oldObj)
	case oldObj == nil && c.owns:
		outcome, err = c.applyCreate(ctx, newObj, true)
	default:
		outcome, err = c.applyUpdate(ctx, oldObj, newObj, c.owns)
	}

	// A throttled apiserver names the delay it wants; carry it to the
	// emitter's backoff.
	if outcome == dbspruntime.Unreachable {
		if secs, ok := apierrors.SuggestsClientDelay(err); ok {
			err = retryAfterError{error: err, delay: time.Duration(secs) * time.Second}
		}
	}
	return outcome, err
}

// retryAfterError carries a plant-suggested delay (an apiserver 429 with a
// Retry-After) alongside the failure it belongs to.
type retryAfterError struct {
	error
	delay time.Duration
}

// RetryAfter implements runtime.RetryAfter.
func (e retryAfterError) RetryAfter() time.Duration { return e.delay }

// Unwrap returns the underlying error.
func (e retryAfterError) Unwrap() error { return e.error }
