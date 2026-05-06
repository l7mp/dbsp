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
		Expect(labels).NotTo(HaveKey("x"))
		Expect(labels).To(HaveKeyWithValue("y", "2"))

		Expect(u.Consume(ctx, out("out", upsert, -1))).To(Succeed())

		obj = keyObject(gvk, "default", "cfg")
		err = c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("updater upsert updates metadata status and body", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

		scheme := kruntime.NewScheme()
		seed := keyObject(gvk, "default", "app")
		seed.Object = map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":        "app",
				"namespace":   "default",
				"labels":      map[string]any{"x": "1"},
				"annotations": map[string]any{"old": "yes"},
				"finalizers":  []any{"cleanup.example.com/old"},
			},
			"spec": map[string]any{"a": int64(1), "b": int64(2)},
			"status": map[string]any{
				"availableReplicas": int64(1),
				"readyReplicas":     int64(1),
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(seed).WithObjects(seed).Build()

		u, err := NewUpdater(Config{Name: "test-updater-status", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		upsert := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":        "app",
				"namespace":   "default",
				"labels":      map[string]any{"y": "2"},
				"annotations": map[string]any{"new": "ok"},
				"finalizers":  []any{"cleanup.example.com/new"},
			},
			"spec": map[string]any{"b": int64(3)},
			"status": map[string]any{
				"availableReplicas": int64(2),
			},
		}

		Expect(u.Consume(ctx, out("out", upsert, 1))).To(Succeed())

		obj := keyObject(gvk, "default", "app")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())

		_, ok, err := unstructured.NestedFieldNoCopy(obj.Object, "spec", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		b, ok, err := unstructured.NestedInt64(obj.Object, "spec", "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(b).To(Equal(int64(3)))

		labels := obj.GetLabels()
		Expect(labels).NotTo(HaveKey("x"))
		Expect(labels).To(HaveKeyWithValue("y", "2"))
		anns := obj.GetAnnotations()
		Expect(anns).NotTo(HaveKey("old"))
		Expect(anns).To(HaveKeyWithValue("new", "ok"))
		fins, ok, err := unstructured.NestedSlice(obj.Object, "metadata", "finalizers")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(fins).To(Equal([]any{"cleanup.example.com/new"}))

		avail, ok, err := unstructured.NestedInt64(obj.Object, "status", "availableReplicas")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(avail).To(Equal(int64(2)))
		_, ok, err = unstructured.NestedFieldNoCopy(obj.Object, "status", "readyReplicas")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
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

	It("patcher upsert does not overwrite unrelated fields", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"}

		scheme := kruntime.NewScheme()
		seed := keyObject(gvk, "default", "svc")
		seed.Object = map[string]any{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      "svc",
				"namespace": "default",
			},
			"spec": map[string]any{"type": "NodePort"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).Build()

		p, err := NewPatcher(Config{Name: "test-patcher-no-clobber", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		patchUpsert := map[string]any{
			"apiVersion": "v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      "svc",
				"namespace": "default",
				"annotations": map[string]any{
					"service-type": "NodePort",
				},
			},
		}

		Expect(p.Consume(ctx, out("out", patchUpsert, 1))).To(Succeed())

		obj := keyObject(gvk, "default", "svc")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())

		typ, ok, err := unstructured.NestedString(obj.Object, "spec", "type")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(typ).To(Equal("NodePort"))

		ann, ok, err := unstructured.NestedString(obj.Object, "metadata", "annotations", "service-type")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(ann).To(Equal("NodePort"))
	})

	It("patcher upsert and delete patch metadata status and body", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

		scheme := kruntime.NewScheme()
		seed := keyObject(gvk, "default", "app")
		seed.Object = map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":        "app",
				"namespace":   "default",
				"annotations": map[string]any{"old": "yes"},
			},
			"spec": map[string]any{"a": int64(1), "b": int64(2)},
			"status": map[string]any{
				"availableReplicas": int64(1),
				"readyReplicas":     int64(1),
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(seed).WithObjects(seed).Build()

		p, err := NewPatcher(Config{Name: "test-patcher-status", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		upsert := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
				"annotations": map[string]any{
					"version": "219",
				},
			},
			"spec": map[string]any{"b": int64(3)},
			"status": map[string]any{
				"availableReplicas": int64(2),
			},
		}

		Expect(p.Consume(ctx, out("out", upsert, 1))).To(Succeed())

		obj := keyObject(gvk, "default", "app")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())

		a, ok, err := unstructured.NestedInt64(obj.Object, "spec", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(a).To(Equal(int64(1)))
		b, ok, err := unstructured.NestedInt64(obj.Object, "spec", "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(b).To(Equal(int64(3)))

		ann, ok, err := unstructured.NestedString(obj.Object, "metadata", "annotations", "version")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(ann).To(Equal("219"))
		oldAnn, ok, err := unstructured.NestedString(obj.Object, "metadata", "annotations", "old")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(oldAnn).To(Equal("yes"))

		avail, ok, err := unstructured.NestedInt64(obj.Object, "status", "availableReplicas")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(avail).To(Equal(int64(2)))
		ready, ok, err := unstructured.NestedInt64(obj.Object, "status", "readyReplicas")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(ready).To(Equal(int64(1)))

		deletePatch := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
				"annotations": map[string]any{
					"version": "219",
				},
			},
			"spec": map[string]any{"b": int64(3)},
			"status": map[string]any{
				"availableReplicas": int64(2),
			},
		}

		Expect(p.Consume(ctx, out("out", deletePatch, -1))).To(Succeed())

		obj = keyObject(gvk, "default", "app")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())

		a, ok, err = unstructured.NestedInt64(obj.Object, "spec", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(a).To(Equal(int64(1)))
		_, ok, err = unstructured.NestedFieldNoCopy(obj.Object, "spec", "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())

		_, ok, err = unstructured.NestedFieldNoCopy(obj.Object, "metadata", "annotations", "version")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		oldAnn, ok, err = unstructured.NestedString(obj.Object, "metadata", "annotations", "old")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(oldAnn).To(Equal("yes"))

		_, ok, err = unstructured.NestedFieldNoCopy(obj.Object, "status", "availableReplicas")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		ready, ok, err = unstructured.NestedInt64(obj.Object, "status", "readyReplicas")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(ready).To(Equal(int64(1)))
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
