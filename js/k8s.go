package js

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	k8sconn "github.com/l7mp/dbsp/connectors/kubernetes"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	enginespec "github.com/l7mp/dbsp/engine/spec"

	"github.com/dop251/goja"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
)

// describeCall summarises the arguments of a goja call, for use in error
// messages when the JS call shape is wrong (e.g. caller passed an opts
// object as the first positional argument). The output is short and
// human-readable: '2 arguments: string "foo", object {gvk, namespace}'.
func describeCall(call goja.FunctionCall) string {
	n := len(call.Arguments)
	if n == 0 {
		return "0 arguments"
	}
	parts := make([]string, n)
	for i, a := range call.Arguments {
		parts[i] = describeArg(a)
	}
	return fmt.Sprintf("%d argument(s): %s", n, strings.Join(parts, ", "))
}

// describeArg returns a short human-readable description of a JS value's
// shape (kind, plus a hint of the contents) for error messages.
func describeArg(a goja.Value) string {
	if a == nil || goja.IsUndefined(a) {
		return "undefined"
	}
	if goja.IsNull(a) {
		return "null"
	}
	if _, ok := goja.AssertFunction(a); ok {
		return "function"
	}
	switch v := a.Export().(type) {
	case string:
		s := v
		if len(s) > 32 {
			s = s[:29] + "..."
		}
		return fmt.Sprintf("string %q", s)
	case bool:
		return fmt.Sprintf("bool %v", v)
	case int64:
		return fmt.Sprintf("number %d", v)
	case float64:
		return fmt.Sprintf("number %v", v)
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(keys) > 5 {
			keys = append(keys[:5:5], "...")
		}
		return fmt.Sprintf("object {%s}", strings.Join(keys, ", "))
	case []any:
		return fmt.Sprintf("array (length=%d)", len(v))
	}
	return a.ExportType().String()
}

func newKubernetesClientset(cfg *rest.Config) (kubernetes.Interface, error) {
	return kubernetes.NewForConfig(cfg)
}

const k8sRuntimeComponentName = "kubernetes-runtime"

type k8sRuntimeRunner struct {
	rt *k8sruntime.Runtime
}

func (r *k8sRuntimeRunner) Name() string { return k8sRuntimeComponentName }

func (r *k8sRuntimeRunner) Start(ctx context.Context) error {
	return r.rt.Start(ctx)
}

// k8sWatch implements kubernetes.watch(topic, opts[, callback]). The
// optional callback has producer semantics: its return value is
// published to topic; returning nothing publishes an empty Z-set.
func (v *VM) k8sWatch(call goja.FunctionCall) (goja.Value, error) {
	kind := "kubernetes.watch"

	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("%s(topic, {gvk, namespace, labels, predicate}[, callback]): expected (string topic, object opts), got %s", kind, describeCall(call))
	}

	if _, ok := call.Argument(0).Export().(string); !ok {
		return nil, fmt.Errorf("%s(topic, {gvk, namespace, labels, predicate}[, callback]): expected (string topic, object opts), got %s", kind, describeCall(call))
	}
	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("%s: empty topic", kind)
	}

	// The options object is the connector's producer wire spec.
	var spec k8sproducer.Spec
	if err := decodeOptionValue(call.Argument(1), &spec); err != nil {
		return nil, fmt.Errorf("%s options: %w", kind, err)
	}

	var callback goja.Callable
	if len(call.Arguments) > 2 {
		arg := call.Argument(2)
		if !goja.IsUndefined(arg) && !goja.IsNull(arg) {
			cb, ok := goja.AssertFunction(arg)
			if !ok {
				return nil, fmt.Errorf("%s callback must be a function", kind)
			}
			callback = cb
		}
	}

	// The verb goes through the loader path: the options become a
	// serialized Source and the connector factory constructs the
	// producer, exactly as a spec-loaded runtime would.
	src, err := v.k8sSourceSpec(spec)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}

	publishTopic := topic
	var callbackStop func()
	if callback != nil {
		publishTopic = v.nextInternalTopic("kubernetes-watch", topic)
		callbackStop = v.registerProducerCallback(publishTopic, topic, "kubernetes-watch-callback", callback)
	}

	runnable, err := v.factoryByName("kubernetes").NewSource(v.runtime, src, publishTopic)
	if err != nil {
		if callbackStop != nil {
			callbackStop()
		}
		return nil, fmt.Errorf("%s: %w", kind, err)
	}
	if err := v.runtime.Add(runnable); err != nil {
		if callbackStop != nil {
			callbackStop()
		}
		return nil, fmt.Errorf("%s: register producer: %w", kind, err)
	}

	return v.boundHandle(runnable, kind, topic, spec, callbackStop), nil
}

// k8sSourceSpec converts the verb options into the serialized Source the
// connector factory consumes, resolving and registering the view GVK the
// way the ambient runtime always has.
func (v *VM) k8sSourceSpec(ps k8sproducer.Spec) (*enginespec.Source, error) {
	res, err := enginespec.ParseResource(ps.GVK)
	if err != nil {
		return nil, err
	}
	if _, err := v.parseGVK(ps.GVK); err != nil {
		return nil, err
	}
	src := enginespec.Source{Resource: res}
	if ps.Namespace != "" {
		ns := ps.Namespace
		src.Namespace = &ns
	}
	if sel := ps.Selector(); sel != nil {
		data, err := json.Marshal(sel)
		if err != nil {
			return nil, err
		}
		raw := json.RawMessage(data)
		src.LabelSelector = &raw
	}
	if ps.Predicate != nil {
		data, err := json.Marshal(ps.Predicate)
		if err != nil {
			return nil, err
		}
		raw := json.RawMessage(data)
		src.Predicate = &raw
	}
	return &src, nil
}

func (v *VM) k8sPatch(call goja.FunctionCall) (goja.Value, error) {
	return v.installK8sConsumer(call, "patcher")
}

func (v *VM) k8sUpdate(call goja.FunctionCall) (goja.Value, error) {
	return v.installK8sConsumer(call, "updater")
}

// installK8sConsumer implements kubernetes.patch(topic, {gvk}) and
// kubernetes.update(topic, {gvk}).
func (v *VM) installK8sConsumer(call goja.FunctionCall, consumerKind string) (goja.Value, error) {
	kind := "kubernetes." + map[string]string{"updater": "update", "patcher": "patch"}[consumerKind]

	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("%s(topic, {gvk}): expected (string topic, object opts), got %s", kind, describeCall(call))
	}

	if _, ok := call.Argument(0).Export().(string); !ok {
		return nil, fmt.Errorf("%s(topic, {gvk}): expected (string topic, object opts), got %s", kind, describeCall(call))
	}
	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("%s: empty topic", kind)
	}

	// The options object is the connector's consumer wire spec.
	var spec k8sconsumer.Spec
	if err := decodeOptionValue(call.Argument(1), &spec); err != nil {
		return nil, fmt.Errorf("%s options: %w", kind, err)
	}
	// The verb goes through the loader path: the options become a
	// serialized Target and the connector factory constructs the
	// consumer, exactly as a spec-loaded runtime would.
	res, err := enginespec.ParseResource(spec.GVK)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}
	if _, err := v.parseGVK(spec.GVK); err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}
	tgt := enginespec.Target{Resource: res, Type: enginespec.Updater}
	if consumerKind == "patcher" {
		tgt.Type = enginespec.Patcher
	}

	consumer, err := v.factoryByName("kubernetes").NewTarget(v.runtime, &tgt, topic)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}
	if err := v.runtime.Add(consumer); err != nil {
		return nil, fmt.Errorf("%s: register consumer: %w", kind, err)
	}

	return v.boundHandle(consumer, kind, topic, spec), nil
}

// factoryByName finds a registered connector factory; the factories are
// installed at VM construction, so a miss is a programming error.
func (v *VM) factoryByName(name string) *dbspruntime.ConnectorFactory {
	for i := range v.factories {
		if v.factories[i].Name == name {
			return &v.factories[i]
		}
	}
	panic(fmt.Sprintf("connector factory %q not registered", name))
}

func (v *VM) ensureK8sRuntime() (*k8sruntime.Runtime, error) {
	v.k8sMu.Lock()
	defer v.k8sMu.Unlock()

	if v.k8sRuntime == nil {
		return nil, fmt.Errorf("kubernetes runtime is not started: call kubernetes.runtime.start() before using kubernetes.watch/patch/update/log")
	}

	return v.k8sRuntime, nil
}

func (v *VM) parseGVK(raw string) (schema.GroupVersionKind, error) {
	res, err := enginespec.ParseResource(raw)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	gvk, err := k8sconn.ResolveResource(v.runtime, v.ensureK8sRuntime, res)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if err := v.ensureK8sViewDiscovery(gvk); err != nil {
		return schema.GroupVersionKind{}, err
	}
	return gvk, nil
}

func (v *VM) k8sRESTMapping(gk schema.GroupKind, version string) (*meta.RESTMapping, error) {
	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return nil, err
	}

	mapping, err := krt.GetRESTMapper().RESTMapping(gk, version)
	if err != nil {
		if !v.k8sNativeAvailable && !viewv1a1.IsViewGroup(gk.Group) {
			return nil, fmt.Errorf("native Kubernetes resources unavailable: kubeconfig is missing, only view resources can be used")
		}
		return nil, fmt.Errorf("resolve GVK for %s/%s: %w", gk.String(), version, err)
	}

	return mapping, nil
}

func (v *VM) ensureK8sViewDiscovery(gvk schema.GroupVersionKind) error {
	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return err
	}
	if err := krt.GetDiscovery().RegisterViewGVK(gvk); err != nil {
		return fmt.Errorf("register view GVK %s: %w", gvk.String(), err)
	}
	return nil
}
