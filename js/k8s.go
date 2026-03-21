package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/dop251/goja"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
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

func (v *VM) k8sWatch(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("producer.kubernetes.watch({ ... }) requires options")
	}

	var opts k8sWatchOptions
	if err := decodeOptionValue(call.Argument(0), &opts); err != nil {
		return nil, fmt.Errorf("producer.kubernetes.watch options: %w", err)
	}
	if opts.Topic == "" {
		return nil, fmt.Errorf("producer.kubernetes.watch: missing topic")
	}

	gvk, err := parseGVK(opts.GVK)
	if err != nil {
		return nil, fmt.Errorf("producer.kubernetes.watch: %w", err)
	}

	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("producer.kubernetes.watch: get kubeconfig: %w", err)
	}

	watcherClient, err := client.NewWithWatch(cfg, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("producer.kubernetes.watch: create watch client: %w", err)
	}

	var selector *v1.LabelSelector
	if len(opts.Labels) > 0 {
		selector = &v1.LabelSelector{MatchLabels: opts.Labels}
	}

	p, err := k8sproducer.NewWatcher(k8sproducer.Config{
		Client:        watcherClient,
		SourceGVK:     gvk,
		InputName:     opts.Topic,
		Namespace:     opts.Namespace,
		LabelSelector: selector,
		Runtime:       v.runtime,
		Logger:        v.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("producer.kubernetes.watch: %w", err)
	}

	v.runtime.Add(p)
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

	gvk, err := parseGVK(opts.GVK)
	if err != nil {
		return nil, fmt.Errorf("consumer.kubernetes: %w", err)
	}

	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("consumer.kubernetes: get kubeconfig: %w", err)
	}

	k8sClient, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("consumer.kubernetes: create client: %w", err)
	}

	baseCfg := k8sconsumer.Config{
		Client:     k8sClient,
		OutputName: opts.Topic,
		TargetGVK:  gvk,
		Logger:     v.logger,
	}

	var consume func(context.Context, dbspruntime.Event) error
	if patcher {
		p, err := k8sconsumer.NewPatcher(baseCfg)
		if err != nil {
			return nil, fmt.Errorf("consumer.kubernetes.patcher: %w", err)
		}
		consume = p.Consume
	} else {
		u, err := k8sconsumer.NewUpdater(baseCfg)
		if err != nil {
			return nil, fmt.Errorf("consumer.kubernetes.updater: %w", err)
		}
		consume = u.Consume
	}

	sub := v.runtime.NewSubscriber()
	sub.Subscribe(opts.Topic)

	go func() {
		for event := range sub.GetChannel() {
			if err := consume(v.ctx, event); err != nil {
				v.logger.Error(err, "kubernetes consumer failed", "topic", opts.Topic)
			}
		}
	}()

	return goja.Undefined(), nil
}

func parseGVK(raw string) (schema.GroupVersionKind, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return schema.GroupVersionKind{}, fmt.Errorf("missing gvk")
	}

	parts := strings.Split(s, "/")
	switch len(parts) {
	case 2:
		gv, err := schema.ParseGroupVersion(parts[0])
		if err != nil {
			return schema.GroupVersionKind{}, fmt.Errorf("gvk apiVersion: %w", err)
		}
		if parts[1] == "" {
			return schema.GroupVersionKind{}, fmt.Errorf("gvk: missing kind")
		}
		return schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: parts[1]}, nil
	case 3:
		if parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return schema.GroupVersionKind{}, fmt.Errorf("gvk: expected group/version/kind")
		}
		return schema.GroupVersionKind{Group: parts[0], Version: parts[1], Kind: parts[2]}, nil
	default:
		return schema.GroupVersionKind{}, fmt.Errorf("gvk must be v1/Kind or group/version/kind")
	}
}
