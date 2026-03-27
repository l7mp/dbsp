package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/dop251/goja"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"
)

type k8sWatchOptions struct {
	GVK       string            `json:"gvk"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels"`
	Topic     string            `json:"topic"`
}

type k8sConsumerOptions struct {
	GVK   string `json:"gvk"`
	Topic string `json:"topic"`
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

func (v *VM) installK8sWatchProducer(call goja.FunctionCall, listMode bool) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		if listMode {
			return nil, fmt.Errorf("producer.kubernetes.list({ ... }) requires options")
		}
		return nil, fmt.Errorf("producer.kubernetes.watch({ ... }) requires options")
	}

	var callback goja.Callable
	if len(call.Arguments) > 1 {
		arg := call.Argument(1)
		if !goja.IsUndefined(arg) && !goja.IsNull(arg) {
			cb, ok := goja.AssertFunction(arg)
			if !ok {
				if listMode {
					return nil, fmt.Errorf("producer.kubernetes.list callback must be a function")
				}
				return nil, fmt.Errorf("producer.kubernetes.watch callback must be a function")
			}
			callback = cb
		}
	}

	var opts k8sWatchOptions
	if err := decodeOptionValue(call.Argument(0), &opts); err != nil {
		if listMode {
			return nil, fmt.Errorf("producer.kubernetes.list options: %w", err)
		}
		return nil, fmt.Errorf("producer.kubernetes.watch options: %w", err)
	}
	if opts.Topic == "" {
		if listMode {
			return nil, fmt.Errorf("producer.kubernetes.list: missing topic")
		}
		return nil, fmt.Errorf("producer.kubernetes.watch: missing topic")
	}

	krt, err := v.ensureK8sRuntime()
	if err != nil {
		if listMode {
			return nil, fmt.Errorf("producer.kubernetes.list: %w", err)
		}
		return nil, fmt.Errorf("producer.kubernetes.watch: %w", err)
	}

	gvk, err := v.parseGVK(opts.GVK)
	if err != nil {
		if listMode {
			return nil, fmt.Errorf("producer.kubernetes.list: %w", err)
		}
		return nil, fmt.Errorf("producer.kubernetes.watch: %w", err)
	}

	var selector *v1.LabelSelector
	if len(opts.Labels) > 0 {
		selector = &v1.LabelSelector{MatchLabels: opts.Labels}
	}

	publishTopic := opts.Topic
	if callback != nil {
		kind := "kubernetes-watch"
		if listMode {
			kind = "kubernetes-list"
		}
		publishTopic = v.nextInternalTopic(kind, opts.Topic)
		v.registerTransformCallbackConsumer(publishTopic, opts.Topic, callback)
	}

	producerKind := "watcher"
	if listMode {
		producerKind = "lister"
	}

	name := fmt.Sprintf("kubernetes-producer-%s-%s-%s", producerKind, opts.Topic, strings.ToLower(gvk.String()))
	baseCfg := k8sproducer.Config{
		Client:        krt.GetClient(),
		SourceGVK:     gvk,
		Name:          name,
		InputName:     publishTopic,
		Namespace:     opts.Namespace,
		LabelSelector: selector,
		Runtime:       v.runtime,
		Logger:        v.logger,
	}

	if listMode {
		p, err := k8sproducer.NewLister(baseCfg)
		if err != nil {
			return nil, fmt.Errorf("producer.kubernetes.list: %w", err)
		}
		if err := v.runtime.Add(p); err != nil {
			return nil, fmt.Errorf("producer.kubernetes.list: register lister: %w", err)
		}
	} else {
		p, err := k8sproducer.NewWatcher(baseCfg)
		if err != nil {
			return nil, fmt.Errorf("producer.kubernetes.watch: %w", err)
		}
		if err := v.runtime.Add(p); err != nil {
			return nil, fmt.Errorf("producer.kubernetes.watch: register watcher: %w", err)
		}
	}

	v.disableIdleDrain("kubernetes source producer")
	return goja.Undefined(), nil
}

func (v *VM) k8sPatcher(call goja.FunctionCall) (goja.Value, error) {
	return v.installK8sConsumer(call, true)
}

func (v *VM) k8sUpdater(call goja.FunctionCall) (goja.Value, error) {
	return v.installK8sConsumer(call, false)
}

func (v *VM) installK8sConsumer(call goja.FunctionCall, patcher bool) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("consumer.kubernetes.*({ ... }) requires options")
	}

	var opts k8sConsumerOptions
	if err := decodeOptionValue(call.Argument(0), &opts); err != nil {
		return nil, fmt.Errorf("consumer.kubernetes options: %w", err)
	}
	if opts.Topic == "" {
		return nil, fmt.Errorf("consumer.kubernetes: missing topic")
	}

	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return nil, fmt.Errorf("consumer.kubernetes: %w", err)
	}

	gvk, err := v.parseGVK(opts.GVK)
	if err != nil {
		return nil, fmt.Errorf("consumer.kubernetes: %w", err)
	}

	consumerKind := "updater"
	if patcher {
		consumerKind = "patcher"
	}

	name := fmt.Sprintf("kubernetes-consumer-%s-%s-%s", consumerKind, opts.Topic, strings.ToLower(gvk.String()))
	baseCfg := k8sconsumer.Config{
		Client:     krt.GetClient(),
		Name:       name,
		OutputName: opts.Topic,
		TargetGVK:  gvk,
		Runtime:    v.runtime,
		Logger:     v.logger,
	}

	if patcher {
		p, err := k8sconsumer.NewPatcher(baseCfg)
		if err != nil {
			return nil, fmt.Errorf("consumer.kubernetes.patcher: %w", err)
		}
		if err := v.runtime.Add(p); err != nil {
			return nil, fmt.Errorf("consumer.kubernetes.patcher: register consumer: %w", err)
		}
	} else {
		u, err := k8sconsumer.NewUpdater(baseCfg)
		if err != nil {
			return nil, fmt.Errorf("consumer.kubernetes.updater: %w", err)
		}
		if err := v.runtime.Add(u); err != nil {
			return nil, fmt.Errorf("consumer.kubernetes.updater: register consumer: %w", err)
		}
	}

	return goja.Undefined(), nil
}

func (v *VM) ensureK8sRuntime() (*k8sruntime.Runtime, error) {
	v.k8sMu.Lock()
	defer v.k8sMu.Unlock()

	if v.k8sRuntime != nil {
		return v.k8sRuntime, nil
	}

	cfg, err := ctrlcfg.GetConfig()
	nativeAvailable := true
	if err != nil {
		if !clientcmd.IsEmptyConfig(err) {
			return nil, fmt.Errorf("get kubeconfig: %w", err)
		}

		nativeAvailable = false
		cfg = nil
		fmt.Fprintln(os.Stderr, "warning: kubeconfig is unavailable: native Kubernetes resources are disabled, only view resources can be used")
	}

	krt, err := k8sruntime.New(k8sruntime.Config{RESTConfig: cfg, Logger: v.logger.WithName("kubernetes-runtime")})
	if err != nil {
		return nil, fmt.Errorf("create runtime: %w", err)
	}

	if nativeAvailable {
		if err := v.runtime.Add(&k8sRuntimeRunner{rt: krt}); err != nil {
			return nil, fmt.Errorf("register runtime: %w", err)
		}
	}

	v.k8sRuntime = krt
	v.k8sNativeAvailable = nativeAvailable

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
