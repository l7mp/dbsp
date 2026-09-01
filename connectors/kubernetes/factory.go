// Package kubernetes provides the Kubernetes connector's binding factory:
// watch sources and update/patch targets for plain resource references
// (Kubernetes API groups and view groups), plus the connector's
// expression operators.
package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	"github.com/l7mp/dbsp/connectors/kubernetes/expressions"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	kruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	kpredicate "github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/spec"
)

// Env is the host environment the factory closes over. Runtime is a lazy
// accessor for the host-owned Kubernetes runtime: the k8s runtime is
// shared across DBSP runtimes and its lifetime belongs to the host, so
// the factory only attaches to it, never starts or stops it.
type Env struct {
	Runtime func() (*kruntime.Runtime, error)
}

// NewFactory returns the Kubernetes connector's binding factory. It
// claims plain resource references: absent groups (the runtime's own view
// group), view groups, and Kubernetes API groups.
func NewFactory(env Env) runtime.ConnectorFactory {
	resolve := func(rt *runtime.Runtime, r spec.Resource) (schema.GroupVersionKind, error) {
		kind := strings.TrimSpace(r.Kind)
		if kind == "" {
			return schema.GroupVersionKind{}, fmt.Errorf("missing kind")
		}
		version := ""
		if r.Version != nil {
			version = strings.TrimSpace(*r.Version)
		}

		if r.Group == nil {
			if rt.Name() == "" {
				return schema.GroupVersionKind{}, fmt.Errorf("a runtime name is required when apiGroup is omitted")
			}
			return viewv1a1.GroupVersionKind(rt.Name(), kind), nil
		}

		group := strings.TrimSpace(*r.Group)
		if group == "" {
			if version == "" {
				version = "v1"
			}
			return schema.GroupVersionKind{Group: "", Version: version, Kind: kind}, nil
		}
		if viewv1a1.IsViewGroup(group) {
			return schema.GroupVersionKind{Group: group, Version: viewv1a1.Version, Kind: kind}, nil
		}
		if version != "" {
			return schema.GroupVersionKind{Group: group, Version: version, Kind: kind}, nil
		}
		krt, err := env.Runtime()
		if err != nil {
			return schema.GroupVersionKind{}, err
		}
		mapping, err := krt.GetRESTMapper().RESTMapping(schema.GroupKind{Group: group, Kind: kind})
		if err != nil {
			return schema.GroupVersionKind{}, fmt.Errorf("resolve GVK for %s/%s: %w", group, kind, err)
		}
		return mapping.GroupVersionKind, nil
	}

	return runtime.ConnectorFactory{
		Name:      "kubernetes",
		Resources: true,
		Init: func(rt *runtime.Runtime, op *spec.RuntimeSpec) error {
			// Register the runtime's own view kinds with the shared k8s
			// runtime; a runtime-owned component unregisters them when
			// the runtime stops.
			seen := map[schema.GroupVersionKind]bool{}
			gvks := []schema.GroupVersionKind{}
			resources := make([]spec.Resource, 0, len(op.Sources)+len(op.Targets))
			for _, s := range op.Sources {
				resources = append(resources, s.Resource)
			}
			for _, t := range op.Targets {
				resources = append(resources, t.Resource)
			}
			for _, r := range resources {
				var gvk schema.GroupVersionKind
				switch {
				case r.Group == nil:
					if rt.Name() == "" {
						return fmt.Errorf("a runtime name is required when apiGroup is omitted")
					}
					gvk = viewv1a1.GroupVersionKind(rt.Name(), strings.TrimSpace(r.Kind))
				case viewv1a1.IsViewGroup(strings.TrimSpace(*r.Group)):
					gvk = schema.GroupVersionKind{Group: strings.TrimSpace(*r.Group), Version: viewv1a1.Version, Kind: strings.TrimSpace(r.Kind)}
				default:
					continue
				}
				if !seen[gvk] {
					seen[gvk] = true
					gvks = append(gvks, gvk)
				}
			}
			if len(gvks) == 0 {
				return nil
			}
			krt, err := env.Runtime()
			if err != nil {
				return err
			}
			for _, gvk := range gvks {
				if err := krt.GetDiscovery().RegisterViewGVK(gvk); err != nil {
					return fmt.Errorf("register discovery GVK %s: %w", gvk.String(), err)
				}
			}
			if api := krt.GetAPIServer(); api != nil {
				if err := api.RegisterGVKs(gvks); err != nil {
					return fmt.Errorf("register API server GVKs: %w", err)
				}
			}
			return rt.Add(&viewRegistration{name: rt.Name() + "/views", krt: krt, gvks: gvks})
		},
		NewSource: func(rt *runtime.Runtime, s *spec.Source, topic string) (runtime.Runnable, error) {
			if s.Type != "" && s.Type != spec.Watcher {
				return nil, fmt.Errorf("unknown source type %q", s.Type)
			}
			gvk, err := resolve(rt, s.Resource)
			if err != nil {
				return nil, err
			}
			pspec := k8sproducer.Spec{GVK: gvkString(gvk), Level: s.Level}
			if s.Namespace != nil {
				pspec.Namespace = *s.Namespace
			}
			if s.LabelSelector != nil {
				var sel metav1.LabelSelector
				if err := json.Unmarshal(*s.LabelSelector, &sel); err != nil {
					return nil, fmt.Errorf("labelSelector: %w", err)
				}
				pspec.LabelSelector = &sel
			}
			if s.Predicate != nil {
				var pred kpredicate.Predicate
				if err := json.Unmarshal(*s.Predicate, &pred); err != nil {
					return nil, fmt.Errorf("predicate: %w", err)
				}
				pspec.Predicate = &pred
			}
			krt, err := env.Runtime()
			if err != nil {
				return nil, err
			}
			client, err := krt.NewCompositeClient()
			if err != nil {
				return nil, fmt.Errorf("connector client: %w", err)
			}
			return k8sproducer.NewFromSpec(topic, pspec, k8sproducer.Deps{
				Client: client, GVK: gvk, Runtime: rt, Logger: rt.Logger(),
			})
		},
		NewTarget: func(rt *runtime.Runtime, t *spec.Target, topic string) (runtime.Runnable, error) {
			verb := "update"
			switch t.Type {
			case "", spec.Updater:
			case spec.Patcher:
				verb = "patch"
			default:
				return nil, fmt.Errorf("unknown target type %q", t.Type)
			}
			gvk, err := resolve(rt, t.Resource)
			if err != nil {
				return nil, err
			}
			krt, err := env.Runtime()
			if err != nil {
				return nil, err
			}
			client, err := krt.NewCompositeClient()
			if err != nil {
				return nil, fmt.Errorf("connector client: %w", err)
			}
			return k8sconsumer.NewFromSpec(topic, verb, k8sconsumer.Spec{GVK: gvkString(gvk), Level: t.Level}, k8sconsumer.Deps{
				Client: client, GVK: gvk, Runtime: rt, Logger: rt.Logger(),
			})
		},
		Expressions: expressions.Ops(),
	}
}

func gvkString(gvk schema.GroupVersionKind) string {
	if gvk.Group == "" {
		return gvk.Version + "/" + gvk.Kind
	}
	return gvk.Group + "/" + gvk.Version + "/" + gvk.Kind
}

// viewRegistration holds a runtime's view-kind registrations in the
// shared k8s runtime and releases them when the runtime stops.
type viewRegistration struct {
	name string
	krt  *kruntime.Runtime
	gvks []schema.GroupVersionKind
}

func (v *viewRegistration) Name() string { return v.name }

func (v *viewRegistration) Start(ctx context.Context) error {
	<-ctx.Done()
	if api := v.krt.GetAPIServer(); api != nil {
		api.UnregisterGVKs(v.gvks)
	}
	for _, gvk := range v.gvks {
		if err := v.krt.GetDiscovery().UnregisterViewGVK(gvk); err != nil {
			return fmt.Errorf("unregister discovery GVK %s: %w", gvk.String(), err)
		}
	}
	return nil
}
