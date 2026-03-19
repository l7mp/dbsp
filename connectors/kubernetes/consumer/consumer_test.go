package consumer

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dbunstructured "github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
)

func TestUpdaterWithFakeClient(t *testing.T) {
	ctx := context.Background()
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

	scheme := kruntime.NewScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	u, err := NewUpdater(Config{Client: c, OutputName: "out", TargetGVK: gvk})
	if err != nil {
		t.Fatalf("NewUpdater failed: %v", err)
	}

	add := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      "cfg",
			"namespace": "default",
			"labels":    map[string]any{"x": "1"},
		},
		"data": map[string]any{"a": "1"},
	}

	if err := u.Consume(ctx, out("out", add, 1)); err != nil {
		t.Fatalf("updater add failed: %v", err)
	}

	obj := keyObject(gvk, "default", "cfg")
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("get after add failed: %v", err)
	}
	if got, ok, _ := unstructured.NestedString(obj.Object, "data", "a"); !ok || got != "1" {
		t.Fatalf("unexpected data.a after add: %v, ok=%v", got, ok)
	}

	upsert := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      "cfg",
			"namespace": "default",
			"labels":    map[string]any{"y": "2"},
		},
		"data": map[string]any{"b": "2"},
	}

	if err := u.Consume(ctx, out("out", upsert, 1)); err != nil {
		t.Fatalf("updater upsert failed: %v", err)
	}

	obj = keyObject(gvk, "default", "cfg")
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("get after upsert failed: %v", err)
	}
	if _, ok, _ := unstructured.NestedString(obj.Object, "data", "a"); ok {
		t.Fatalf("expected data.a removed by updater")
	}
	if got, ok, _ := unstructured.NestedString(obj.Object, "data", "b"); !ok || got != "2" {
		t.Fatalf("unexpected data.b after upsert: %v, ok=%v", got, ok)
	}
	if labels := obj.GetLabels(); labels["x"] != "1" || labels["y"] != "2" {
		t.Fatalf("unexpected merged labels: %#v", labels)
	}

	if err := u.Consume(ctx, out("out", upsert, -1)); err != nil {
		t.Fatalf("updater delete failed: %v", err)
	}

	obj = keyObject(gvk, "default", "cfg")
	err = c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected not found after updater delete, got: %v", err)
	}
}

func TestPatcherWithFakeClient(t *testing.T) {
	ctx := context.Background()
	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

	scheme := kruntime.NewScheme()
	seed := keyObject(gvk, "default", "app")
	seed.Object = map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "app",
			"namespace": "default",
		},
		"spec": map[string]any{"a": int64(1), "b": int64(2)},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).Build()

	p, err := NewPatcher(Config{Client: c, OutputName: "out", TargetGVK: gvk})
	if err != nil {
		t.Fatalf("NewPatcher failed: %v", err)
	}

	patchUpsert := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "app",
			"namespace": "default",
		},
		"spec": map[string]any{"b": int64(3)},
	}

	if err := p.Consume(ctx, out("out", patchUpsert, 1)); err != nil {
		t.Fatalf("patcher upsert failed: %v", err)
	}

	obj := keyObject(gvk, "default", "app")
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("get after patch upsert failed: %v", err)
	}
	if got, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "a"); !ok || got != 1 {
		t.Fatalf("expected spec.a retained, got: %v, ok=%v", got, ok)
	}
	if got, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "b"); !ok || got != 3 {
		t.Fatalf("expected spec.b patched to 3, got: %v, ok=%v", got, ok)
	}

	patchDelete := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "app",
			"namespace": "default",
		},
		"spec": map[string]any{"b": int64(3)},
	}

	if err := p.Consume(ctx, out("out", patchDelete, -1)); err != nil {
		t.Fatalf("patcher delete-patch failed: %v", err)
	}

	obj = keyObject(gvk, "default", "app")
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("get after patch delete failed: %v", err)
	}
	if _, ok, _ := unstructured.NestedFieldNoCopy(obj.Object, "spec", "b"); ok {
		t.Fatalf("expected spec.b removed by delete-patch")
	}
	if got, ok, _ := unstructured.NestedInt64(obj.Object, "spec", "a"); !ok || got != 1 {
		t.Fatalf("expected spec.a retained after delete-patch, got: %v, ok=%v", got, ok)
	}
}

func out(name string, doc map[string]any, w zset.Weight) dbspruntime.Output {
	z := zset.New()
	z.Insert(dbunstructured.New(doc, nil), w)
	return dbspruntime.Output{Name: name, Data: z}
}

func keyObject(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}
