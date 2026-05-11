package js

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/dop251/goja"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	kpredicate "github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
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

type k8sWatchOptions struct {
	GVK       string                `json:"gvk"`
	Namespace string                `json:"namespace"`
	Labels    map[string]string     `json:"labels"`
	Predicate *kpredicate.Predicate `json:"predicate"`
}

type k8sConsumerOptions struct {
	GVK string `json:"gvk"`
}

const k8sRuntimeComponentName = "kubernetes-runtime"

type k8sRuntimeRunner struct {
	rt *k8sruntime.Runtime
}

func (r *k8sRuntimeRunner) Name() string { return k8sRuntimeComponentName }

func (r *k8sRuntimeRunner) Start(ctx context.Context) error {
	return r.rt.Start(ctx)
}

func (v *VM) k8sWatch(call goja.FunctionCall) (goja.Value, error) {
	return v.installK8sWatchProducer(call, false)
}

func (v *VM) k8sList(call goja.FunctionCall) (goja.Value, error) {
	return v.installK8sWatchProducer(call, true)
}

// installK8sWatchProducer implements kubernetes.watch(topic, opts[, callback]) and
// kubernetes.list(topic, opts[, callback]).  The optional callback has producer
// semantics: its return value is published to topic; returning nothing publishes
// an empty Z-set.
func (v *VM) installK8sWatchProducer(call goja.FunctionCall, listMode bool) (goja.Value, error) {
	kind := "kubernetes.watch"
	if listMode {
		kind = "kubernetes.list"
	}

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

	var opts k8sWatchOptions
	if err := decodeOptionValue(call.Argument(1), &opts); err != nil {
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

	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}

	gvk, err := v.parseGVK(opts.GVK)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}

	var selector *v1.LabelSelector
	if len(opts.Labels) > 0 {
		selector = &v1.LabelSelector{MatchLabels: opts.Labels}
	}

	publishTopic := topic
	var callbackStop func()
	if callback != nil {
		internalKind := "kubernetes-watch"
		if listMode {
			internalKind = "kubernetes-list"
		}
		publishTopic = v.nextInternalTopic(internalKind, topic)
		callbackStop = v.registerProducerCallback(publishTopic, topic, internalKind+"-callback", callback)
	}

	producerKind := "watcher"
	if listMode {
		producerKind = "lister"
	}

	name := fmt.Sprintf("kubernetes-producer-%s-%s-%s", producerKind, topic, strings.ToLower(gvk.String()))
	baseCfg := k8sproducer.Config{
		Client:        krt.GetClient(),
		SourceGVK:     gvk,
		Name:          name,
		InputName:     publishTopic,
		Namespace:     opts.Namespace,
		LabelSelector: selector,
		Predicate:     opts.Predicate,
		Runtime:       v.runtime,
		Logger:        v.logger,
	}

	var runnable dbspruntime.Runnable
	if listMode {
		p, err := k8sproducer.NewLister(baseCfg)
		if err != nil {
			if callbackStop != nil {
				callbackStop()
			}
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		if err := v.runtime.Add(p); err != nil {
			if callbackStop != nil {
				callbackStop()
			}
			return nil, fmt.Errorf("%s: register lister: %w", kind, err)
		}
		runnable = p
	} else {
		p, err := k8sproducer.NewWatcher(baseCfg)
		if err != nil {
			if callbackStop != nil {
				callbackStop()
			}
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		if err := v.runtime.Add(p); err != nil {
			if callbackStop != nil {
				callbackStop()
			}
			return nil, fmt.Errorf("%s: register watcher: %w", kind, err)
		}
		runnable = p
	}

	return v.runnableHandle(runnable, callbackStop), nil
}

func (v *VM) k8sPatch(call goja.FunctionCall) (goja.Value, error) {
	return v.installK8sConsumer(call, true)
}

func (v *VM) k8sUpdate(call goja.FunctionCall) (goja.Value, error) {
	return v.installK8sConsumer(call, false)
}

// installK8sConsumer implements kubernetes.patch(topic, {gvk}) and
// kubernetes.update(topic, {gvk}).
func (v *VM) installK8sConsumer(call goja.FunctionCall, patcher bool) (goja.Value, error) {
	kind := "kubernetes.update"
	if patcher {
		kind = "kubernetes.patch"
	}

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

	var opts k8sConsumerOptions
	if err := decodeOptionValue(call.Argument(1), &opts); err != nil {
		return nil, fmt.Errorf("%s options: %w", kind, err)
	}

	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}

	gvk, err := v.parseGVK(opts.GVK)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}

	consumerKind := "updater"
	if patcher {
		consumerKind = "patcher"
	}

	name := fmt.Sprintf("kubernetes-consumer-%s-%s-%s", consumerKind, topic, strings.ToLower(gvk.String()))
	baseCfg := k8sconsumer.Config{
		Client:     krt.GetClient(),
		Name:       name,
		OutputName: topic,
		TargetGVK:  gvk,
		Runtime:    v.runtime,
		Logger:     v.logger,
	}

	var runnable dbspruntime.Runnable
	if patcher {
		p, err := k8sconsumer.NewPatcher(baseCfg)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		if err := v.runtime.Add(p); err != nil {
			return nil, fmt.Errorf("%s: register consumer: %w", kind, err)
		}
		runnable = p
	} else {
		u, err := k8sconsumer.NewUpdater(baseCfg)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		if err := v.runtime.Add(u); err != nil {
			return nil, fmt.Errorf("%s: register consumer: %w", kind, err)
		}
		runnable = u
	}

	return v.runnableHandle(runnable), nil
}

func (v *VM) ensureK8sRuntime() (*k8sruntime.Runtime, error) {
	v.k8sMu.Lock()
	defer v.k8sMu.Unlock()

	if v.k8sRuntime == nil {
		return nil, fmt.Errorf("kubernetes runtime is not started: call kubernetes.runtime.start() before using kubernetes.watch/list/patch/update/log")
	}

	return v.k8sRuntime, nil
}

func (v *VM) parseGVK(raw string) (schema.GroupVersionKind, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return schema.GroupVersionKind{}, fmt.Errorf("missing gvk")
	}

	var gk schema.GroupKind
	var version string

	parts := strings.Split(s, "/")
	switch len(parts) {
	case 2:
		gv, err := schema.ParseGroupVersion(parts[0])
		if err != nil {
			return schema.GroupVersionKind{}, fmt.Errorf("gvk apiVersion: %w", err)
		}
		kind := strings.TrimSpace(parts[1])
		if kind == "" {
			return schema.GroupVersionKind{}, fmt.Errorf("gvk: missing kind")
		}
		gk = schema.GroupKind{Group: gv.Group, Kind: kind}
		version = gv.Version
	case 3:
		group := strings.TrimSpace(parts[0])
		version = strings.TrimSpace(parts[1])
		kind := strings.TrimSpace(parts[2])
		if group == "" || version == "" || kind == "" {
			return schema.GroupVersionKind{}, fmt.Errorf("gvk: expected group/version/kind")
		}
		gk = schema.GroupKind{Group: group, Kind: kind}
	default:
		return schema.GroupVersionKind{}, fmt.Errorf("gvk must be v1/Kind or group/version/kind")
	}

	mapping, err := v.k8sRESTMapping(gk, version)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	gvk := mapping.GroupVersionKind
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
