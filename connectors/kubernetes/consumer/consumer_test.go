package consumer

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Kubernetes consumers", func() {
	It("updates and deletes objects with updater", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		u, err := NewUpdater(Config{Client: c, OutputName: "out", TargetGVK: gvk})
		Expect(err).NotTo(HaveOccurred())

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

		Expect(u.Consume(ctx, out("out", add, 1))).To(Succeed())

		obj := keyObject(gvk, "default", "cfg")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		gotA, ok, err := unstructured.NestedString(obj.Object, "data", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(gotA).To(Equal("1"))

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

		Expect(u.Consume(ctx, out("out", upsert, 1))).To(Succeed())

		obj = keyObject(gvk, "default", "cfg")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		_, ok, err = unstructured.NestedString(obj.Object, "data", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		gotB, ok, err := unstructured.NestedString(obj.Object, "data", "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(gotB).To(Equal("2"))
		labels := obj.GetLabels()
		Expect(labels["x"]).To(Equal("1"))
		Expect(labels["y"]).To(Equal("2"))

		Expect(u.Consume(ctx, out("out", upsert, -1))).To(Succeed())

		obj = keyObject(gvk, "default", "cfg")
		err = c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("patches and unpatches objects with patcher", func() {
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
		Expect(err).NotTo(HaveOccurred())

		patchUpsert := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{"b": int64(3)},
		}

		Expect(p.Consume(ctx, out("out", patchUpsert, 1))).To(Succeed())

		obj := keyObject(gvk, "default", "app")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		gotA, ok, err := unstructured.NestedInt64(obj.Object, "spec", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(gotA).To(Equal(int64(1)))
		gotB, ok, err := unstructured.NestedInt64(obj.Object, "spec", "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(gotB).To(Equal(int64(3)))

		patchDelete := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{"b": int64(3)},
		}

		Expect(p.Consume(ctx, out("out", patchDelete, -1))).To(Succeed())

		obj = keyObject(gvk, "default", "app")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		_, ok, err = unstructured.NestedFieldNoCopy(obj.Object, "spec", "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		gotA, ok, err = unstructured.NestedInt64(obj.Object, "spec", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(gotA).To(Equal(int64(1)))
	})
})

func out(name string, doc map[string]any, w zset.Weight) dbspruntime.Event {
	z := zset.New()
	z.Insert(dbunstructured.New(doc, nil), w)
	return dbspruntime.Event{Name: name, Data: z}
}

func keyObject(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}
