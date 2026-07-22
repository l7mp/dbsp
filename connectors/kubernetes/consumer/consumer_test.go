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

	It("passes status through updater update payload for native objects", func() {
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
			"spec": map[string]any{"replicas": int64(1)},
			"status": map[string]any{
				"availableReplicas": int64(1),
			},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(seed).WithObjects(seed).Build()
		recording := &recordingClient{Client: base}

		u, err := NewUpdater(Config{Name: "test-updater-payload-status", Client: recording, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		upsert := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{"replicas": int64(2)},
			"status": map[string]any{
				"availableReplicas": int64(2),
			},
		}

		Expect(u.Consume(ctx, out("out", upsert, 1))).To(Succeed())
		Expect(recording.lastUpdated).NotTo(BeNil())

		updated, ok := recording.lastUpdated.(*unstructured.Unstructured)
		Expect(ok).To(BeTrue())
		avail, ok, err := unstructured.NestedInt64(updated.Object, "status", "availableReplicas")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(avail).To(Equal(int64(2)))
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

type recordingClient struct {
	client.Client
	lastUpdated client.Object
}

func (c *recordingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if o, ok := obj.DeepCopyObject().(client.Object); ok {
		c.lastUpdated = o
	} else {
		c.lastUpdated = obj
	}

	return c.Client.Update(ctx, obj, opts...)
}

func out(name string, doc map[string]any, w zset.Weight) dbspruntime.Event {
	z := zset.New()
	z.Insert(dbspunstructured.New(doc), w)
	return dbspruntime.Event{Name: name, Data: z}
}

func outMany(name string, entries ...docWeight) dbspruntime.Event {
	z := zset.New()
	for _, e := range entries {
		z.Insert(dbspunstructured.New(e.doc), e.w)
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

var _ = Describe("Kubernetes setter", func() {
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

	cm := func(name string, labels map[string]any, data map[string]any) map[string]any {
		meta := map[string]any{"name": name, "namespace": "default"}
		if labels != nil {
			meta["labels"] = labels
		}
		return map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   meta,
			"data":       data,
		}
	}

	level := func(topic string, docs ...map[string]any) dbspruntime.Event {
		z := zset.New()
		for _, d := range docs {
			z.Insert(dbspunstructured.New(d), 1)
		}
		return dbspruntime.Event{Name: topic, Data: z}
	}

	seed := func(content map[string]any) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetUnstructuredContent(content)
		obj.SetGroupVersionKind(gvk)
		return obj
	}

	It("reconciles the managed scope to the level", func() {
		ctx := context.Background()
		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(
				seed(cm("stale", nil, map[string]any{"a": "old"})),
				seed(cm("extra", nil, map[string]any{"b": "1"})),
			).Build()

		st, err := NewSetter(Config{Name: "test-setter", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		Expect(st.Consume(ctx, level("out",
			cm("stale", nil, map[string]any{"a": "new"}),
			cm("fresh", nil, map[string]any{"c": "1"}),
		))).To(Succeed())

		obj := keyObject(gvk, "default", "stale")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		got, _, err := unstructured.NestedString(obj.Object, "data", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("new"))

		obj = keyObject(gvk, "default", "fresh")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())

		obj = keyObject(gvk, "default", "extra")
		err = c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "extra object must be deleted")
	})

	It("skips the write when the content already matches", func() {
		ctx := context.Background()
		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(seed(cm("same", nil, map[string]any{"a": "1"}))).Build()

		st, err := NewSetter(Config{Name: "test-setter", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		obj := keyObject(gvk, "default", "same")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		before := obj.GetResourceVersion()

		Expect(st.Consume(ctx, level("out", cm("same", nil, map[string]any{"a": "1"})))).To(Succeed())

		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		Expect(obj.GetResourceVersion()).To(Equal(before), "identical content must not be rewritten")
	})

	It("owns the entire target kind", func() {
		ctx := context.Background()
		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(
				seed(cm("one", map[string]any{"app": "x"}, map[string]any{"a": "1"})),
				seed(cm("other", nil, map[string]any{"b": "1"})),
			).Build()

		st, err := NewSetter(Config{Name: "test-setter", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		// An empty level empties the whole kind: the Setter owns it.
		Expect(st.Consume(ctx, dbspruntime.Event{Name: "out", Data: zset.New()})).To(Succeed())

		obj := keyObject(gvk, "default", "one")
		Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(obj), obj))).To(BeTrue())
		obj = keyObject(gvk, "default", "other")
		Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(obj), obj))).To(BeTrue())
	})

	It("compares wholesale: a status difference triggers the write", func() {
		ctx := context.Background()
		dgvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

		seedObj := &unstructured.Unstructured{}
		seedObj.SetUnstructuredContent(map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata":   map[string]any{"name": "app", "namespace": "default"},
			"spec":       map[string]any{"replicas": int64(1)},
			"status":     map[string]any{"readyReplicas": int64(0)},
		})

		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(seedObj).WithObjects(seedObj).Build()

		st, err := NewSetter(Config{Name: "test-setter-status", Client: c, OutputName: "out", TargetGVK: dgvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		// Same spec, different status: wholesale comparison must write it.
		z := zset.New()
		z.Insert(dbspunstructured.New(map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata":   map[string]any{"name": "app", "namespace": "default"},
			"spec":       map[string]any{"replicas": int64(1)},
			"status":     map[string]any{"readyReplicas": int64(1)},
		}), 1)
		Expect(st.Consume(ctx, dbspruntime.Event{Name: "out", Data: z})).To(Succeed())

		obj := keyObject(dgvk, "default", "app")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		ready, _, err := unstructured.NestedInt64(obj.Object, "status", "readyReplicas")
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(Equal(int64(1)))
	})

	It("rejects retractions in level events", func() {
		ctx := context.Background()
		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		st, err := NewSetter(Config{Name: "test-setter", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		z := zset.New()
		z.Insert(dbspunstructured.New(cm("neg", nil, nil)), -1)
		err = st.Consume(ctx, dbspruntime.Event{Name: "out", Data: z})
		Expect(err).To(MatchError(ContainSubstring("no retractions")))
	})
})
