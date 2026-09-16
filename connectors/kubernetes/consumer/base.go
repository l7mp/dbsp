package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/engine/datamodel"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Retry schedule for unreachable-plant failures: exponential with jitter
// from the initial delay to the cap, the same shape the producer's watch
// reconnect uses. The jitter keeps the consumers of a throttled apiserver
// from retrying in lockstep. Assigning the template copies it, so every
// stall starts from a fresh schedule.
var retryBackoff = wait.Backoff{
	Duration: time.Second,
	Factor:   2,
	Jitter:   0.1,
	Cap:      30 * time.Second,
	Steps:    math.MaxInt32,
}

// Config configures Kubernetes consumers.
type Config struct {
	Client client.Client

	// Name is the unique component name used for error reporting. Required.
	Name       string
	OutputName string
	TargetGVK  schema.GroupVersionKind

	// Converter translates pipeline documents into objects to write.
	// Defaults to the connector's table converter.
	Converter kobject.Converter

	// Runtime is the engine runtime used to create a subscriber.
	Runtime *dbspruntime.Runtime

	Logger logr.Logger
}

// baseConsumer is the shared write path. The connector choice declares ownership (owns): an
// Updater owns the objects it writes, so it creates them when absent and deletes them on
// retraction; a Patcher decorates somebody else's objects, so it only ever patches fields and
// never creates/deletes objects. Updates are RFC 7386 merge patches of the (old, new) pair the
// write path derives per object.
type baseConsumer struct {
	*dbspruntime.BaseConsumer

	client     client.Client
	outputName string
	targetGVK  schema.GroupVersionKind
	converter  kobject.Converter
	log        logr.Logger
	owns       bool

	// pump is the write path: Consume feeds it the output deltas as
	// outstanding jobs, its emitter drains them into the apiserver.
	pump *dbspruntime.WritePump
}

// MarshalJSON provides a stable machine-readable representation.
func (c *baseConsumer) MarshalJSON() ([]byte, error) {
	if c == nil {
		return json.Marshal(map[string]any{"component": "consumer", "type": "kubernetes", "nil": true})
	}

	return json.Marshal(map[string]any{
		"component": "consumer",
		"type":      "kubernetes",
		"name":      c.Name(),
		"topic":     c.outputName,
		"targetGVK": c.targetGVK.String(),
	})
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

	converter := cfg.Converter
	if converter == nil {
		converter = kobject.DefaultConverter
	}

	b := &baseConsumer{
		BaseConsumer: base,
		client:       cfg.Client,
		outputName:   cfg.OutputName,
		targetGVK:    cfg.TargetGVK,
		converter:    converter,
		log:          log,
	}

	b.pump = dbspruntime.NewWritePump(dbspruntime.WritePumpConfig{
		Adapter: b,
		Report:  b.HandleError,
		NewBackoff: func() dbspruntime.Backoff {
			schedule := retryBackoff
			return &schedule
		},
		Logger: log,
	})

	return b, nil
}

// start is the shared event loop for all consumers: the emitter drains
// the write pump in the background while consume takes every event off the
// subscriber channel. Consume errors are non-critical: they are reported
// via the runtime error channel and the consumer continues processing
// subsequent events.
func (c *baseConsumer) start(ctx context.Context, consume dbspruntime.ConsumeHandler) error {
	go func() {
		if err := c.pump.Run(ctx); err != nil {
			c.HandleError(err)
		}
	}()

	return c.Run(ctx, consume)
}

// consume feeds one output event into the write pump: adapt, pair by
// plant key, absorb into the outstanding jobs. It never touches the
// apiserver, so a plant that stops answering cannot stall the circuit
// publishing to this topic.
func (c *baseConsumer) consume(_ context.Context, out dbspruntime.Event) error {
	if err := c.pump.Feed(out.Data); err != nil {
		return fmt.Errorf("consumer %s: %w", c.Name(), err)
	}
	return nil
}

// applyUpdate patches the target with the merge diff of the pair. Foreign
// fields survive: a diff-derived patch cannot touch a field the pipeline
// never wrote. When the target is gone and the consumer owns it, the
// object is recreated (resurrection is the feature for generated objects);
// a decorator reports and drops.
func (c *baseConsumer) applyUpdate(ctx context.Context, oldObj, newObj kobject.Object, allowCreate bool) (dbspruntime.ApplyResult, error) {
	view := isViewObject(newObj)
	key := client.ObjectKeyFromObject(newObj).String()
	ps, err := writePatches(oldObj, newObj, view)
	if err != nil {
		return dbspruntime.Refused, fmt.Errorf("consumer %s: diff %s: %w", c.Name(), key, err)
	}

	if len(ps.main) > 0 {
		err := c.patch(ctx, newObj, ps.main, false)
		if apierrors.IsNotFound(err) {
			if allowCreate {
				return c.applyCreate(ctx, newObj, false)
			}
			return dbspruntime.Refused, fmt.Errorf("consumer %s: patch %s: %w", c.Name(), key, err)
		}
		if err != nil {
			return classifyWriteError(err), fmt.Errorf("consumer %s: patch %s: %w", c.Name(), key, err)
		}
	}

	if len(ps.status) > 0 {
		err := c.patch(ctx, newObj, ps.status, true)
		if apierrors.IsNotFound(err) && allowCreate {
			return c.applyCreate(ctx, newObj, false)
		}
		if err != nil {
			return classifyWriteError(err), fmt.Errorf("consumer %s: patch %s status: %w", c.Name(), key, err)
		}
	}

	return dbspruntime.Applied, nil
}

// applyCreate creates the asserted object. On AlreadyExists the create
// falls back to a patch of all asserted fields exactly once: the delta
// carried no old state, so there is nothing to diff against and the whole
// assertion is applied.
func (c *baseConsumer) applyCreate(ctx context.Context, newObj kobject.Object, retryAsPatch bool) (dbspruntime.ApplyResult, error) {
	view := isViewObject(newObj)
	key := client.ObjectKeyFromObject(newObj).String()

	content := kruntime.DeepCopyJSON(newObj.UnstructuredContent())
	kobject.StripOnWrite(content)
	statusValue, hasStatus := content["status"]
	if hasStatus && !view {
		delete(content, "status")
	}

	obj := &unstructured.Unstructured{}
	obj.SetUnstructuredContent(content)
	obj.SetGroupVersionKind(newObj.GroupVersionKind())
	obj.SetName(newObj.GetName())
	obj.SetNamespace(newObj.GetNamespace())

	if err := c.client.Create(ctx, obj); err != nil {
		if apierrors.IsAlreadyExists(err) && retryAsPatch {
			return c.applyUpdate(ctx, nil, newObj, false)
		}
		return classifyWriteError(err), fmt.Errorf("consumer %s: create %s: %w", c.Name(), key, err)
	}

	if hasStatus && !view {
		body := map[string]any{"status": statusValue}
		if err := c.patch(ctx, newObj, body, true); err != nil {
			return classifyWriteError(err), fmt.Errorf("consumer %s: create %s status: %w", c.Name(), key, err)
		}
	}

	return dbspruntime.Applied, nil
}

// applyRetract handles a bare retraction: the owner deletes the object,
// the decorator clears the fields it held. A missing target means the
// retraction is moot either way.
func (c *baseConsumer) applyRetract(ctx context.Context, oldObj kobject.Object) (dbspruntime.ApplyResult, error) {
	key := client.ObjectKeyFromObject(oldObj).String()

	if c.owns {
		if err := c.client.Delete(ctx, identityObject(oldObj)); err != nil && !apierrors.IsNotFound(err) {
			return classifyWriteError(err), fmt.Errorf("consumer %s: delete %s: %w", c.Name(), key, err)
		}
		return dbspruntime.Applied, nil
	}

	view := isViewObject(oldObj)
	ps, err := writePatches(oldObj, nil, view)
	if err != nil {
		return dbspruntime.Refused, fmt.Errorf("consumer %s: diff %s: %w", c.Name(), key, err)
	}

	if len(ps.main) > 0 {
		err := c.patch(ctx, oldObj, ps.main, false)
		if err != nil && !apierrors.IsNotFound(err) {
			return classifyWriteError(err), fmt.Errorf("consumer %s: retract %s: %w", c.Name(), key, err)
		}
	}
	if len(ps.status) > 0 {
		err := c.patch(ctx, oldObj, ps.status, true)
		if err != nil && !apierrors.IsNotFound(err) {
			return classifyWriteError(err), fmt.Errorf("consumer %s: retract %s status: %w", c.Name(), key, err)
		}
	}

	return dbspruntime.Applied, nil
}

// patch sends one merge patch to the main resource or the status
// subresource.
func (c *baseConsumer) patch(ctx context.Context, target kobject.Object, body map[string]any, status bool) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	obj := identityObject(target)
	if status {
		return c.client.Status().Patch(ctx, obj, client.RawPatch(types.MergePatchType, raw))
	}
	return c.client.Patch(ctx, obj, client.RawPatch(types.MergePatchType, raw))
}

// patchSet is the wire form of one pair: the main and status merge
// patches.
type patchSet struct {
	main   map[string]any
	status map[string]any
}

// writePatches builds the main and status merge patches for a pair. Either
// side may be nil (bare assertion or retraction). Identity fields are
// stripped before diffing: they address the request and never appear in a
// patch body. View objects have no status subresource, so their status
// rides the main patch.
func writePatches(oldObj, newObj kobject.Object, view bool) (patchSet, error) {
	oldContent := writeContent(oldObj)
	newContent := writeContent(newObj)

	if view {
		mainPatch, err := datamodel.CreateMergePatch(oldContent, newContent)
		return patchSet{main: mainPatch}, err
	}

	oldStatus := splitStatus(oldContent)
	newStatus := splitStatus(newContent)

	mainPatch, err := datamodel.CreateMergePatch(oldContent, newContent)
	if err != nil {
		return patchSet{}, err
	}
	statusPatch, err := datamodel.CreateMergePatch(oldStatus, newStatus)
	if err != nil {
		return patchSet{}, err
	}
	return patchSet{main: mainPatch, status: statusPatch}, nil
}

// splitStatus removes the status from a write content and returns it
// wrapped for the subresource diff.
func splitStatus(content map[string]any) map[string]any {
	if content == nil {
		return nil
	}
	status, ok := content["status"]
	if !ok {
		return map[string]any{}
	}
	delete(content, "status")
	return map[string]any{"status": status}
}

// writeContent returns the object's content as the diff sees it:
// server-owned fields stripped per the API-field table, identity (GVK,
// name, namespace) removed. Nil-safe: a nil object diffs as an empty
// document.
func writeContent(obj kobject.Object) map[string]any {
	if obj == nil {
		return nil
	}
	content := kruntime.DeepCopyJSON(obj.UnstructuredContent())
	delete(content, "apiVersion")
	delete(content, "kind")
	kobject.StripOnWrite(content)
	if meta, ok := content["metadata"].(map[string]any); ok {
		delete(meta, "name")
		delete(meta, "namespace")
		if len(meta) == 0 {
			delete(content, "metadata")
		}
	}
	return content
}

// identityObject returns a fresh object carrying only the addressing
// identity of the target.
func identityObject(obj kobject.Object) *unstructured.Unstructured {
	out := &unstructured.Unstructured{}
	out.SetGroupVersionKind(obj.GroupVersionKind())
	out.SetName(obj.GetName())
	out.SetNamespace(obj.GetNamespace())
	return out
}

// classifyWriteError maps a client error onto the write outcome contract.
// Unreachable covers exactly two shapes: the plant asked us to come back
// later (throttling and server-overload statuses) and the command
// demonstrably never got an answer (transport-level failures). Everything
// else is refused: an unrecognized failure is reported and dropped rather
// than silently retried, because a retry loop hides it while a report
// surfaces it, and only one of those is diagnosable. Client-side rejections
// (a write the client refuses to even send) land here too.
func classifyWriteError(err error) dbspruntime.ApplyResult {
	if err == nil {
		return dbspruntime.Applied
	}
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) ||
		apierrors.IsInternalError(err) {
		return dbspruntime.Unreachable
	}
	if _, ok := apierrors.SuggestsClientDelay(err); ok {
		return dbspruntime.Unreachable
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return dbspruntime.Unreachable
	}
	return dbspruntime.Refused
}

func isViewObject(obj client.Object) bool {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return viewv1a1.IsViewKind(gvk)
}
