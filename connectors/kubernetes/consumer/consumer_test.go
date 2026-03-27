package consumer

import (
	"context"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Kubernetes consumers", func() {
	It("updates and deletes objects with updater", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		u, err := NewUpdater(Config{Name: "test-updater", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
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

		p, err := NewPatcher(Config{Name: "test-patcher", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
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

	It("collapses mixed add and delete for one key into one patch update", func() {
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
			"spec": map[string]any{
				"template": map[string]any{
					"metadata": map[string]any{
						"annotations": map[string]any{"dcontroller.io/configmap-version": "210"},
					},
				},
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).Build()

		p, err := NewPatcher(Config{Name: "test-patcher-collapse", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		oldDoc := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{
				"template": map[string]any{
					"metadata": map[string]any{
						"annotations": map[string]any{"dcontroller.io/configmap-version": "210"},
					},
				},
			},
		}
		newDoc := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{
				"template": map[string]any{
					"metadata": map[string]any{
						"annotations": map[string]any{"dcontroller.io/configmap-version": "219"},
					},
				},
			},
		}

		Expect(p.Consume(ctx, outMany("out",
			docWeight{doc: newDoc, w: 1},
			docWeight{doc: oldDoc, w: -1},
		))).To(Succeed())

		obj := keyObject(gvk, "default", "app")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		version, ok, err := unstructured.NestedString(obj.Object, "spec", "template", "metadata", "annotations", "dcontroller.io/configmap-version")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(version).To(Equal("219"))
	})

	It("collapses mixed add and delete for one key into one updater upsert", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		scheme := kruntime.NewScheme()
		seed := keyObject(gvk, "default", "cfg")
		seed.Object = map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "cfg",
				"namespace": "default",
			},
			"data": map[string]any{"version": "210"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).Build()

		u, err := NewUpdater(Config{Name: "test-updater-collapse", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		oldDoc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "cfg",
				"namespace": "default",
			},
			"data": map[string]any{"version": "210"},
		}
		newDoc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "cfg",
				"namespace": "default",
			},
			"data": map[string]any{"version": "219"},
		}

		Expect(u.Consume(ctx, outMany("out",
			docWeight{doc: newDoc, w: 1},
			docWeight{doc: oldDoc, w: -1},
		))).To(Succeed())

		obj := keyObject(gvk, "default", "cfg")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		version, ok, err := unstructured.NestedString(obj.Object, "data", "version")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(version).To(Equal("219"))
	})
})

type docWeight struct {
	doc map[string]any
	w   zset.Weight
}

func out(name string, doc map[string]any, w zset.Weight) dbspruntime.Event {
	z := zset.New()
	z.Insert(dbspunstructured.New(doc, nil), w)
	return dbspruntime.Event{Name: name, Data: z}
}

func outMany(name string, entries ...docWeight) dbspruntime.Event {
	z := zset.New()
	for _, e := range entries {
		z.Insert(dbspunstructured.New(e.doc, nil), e.w)
	}
	return dbspruntime.Event{Name: name, Data: z}
}

func keyObject(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}
