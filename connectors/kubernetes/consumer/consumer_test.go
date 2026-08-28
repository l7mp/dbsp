package consumer

import (
	"context"
	"encoding/json"
	"time"

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

		Expect(u.Consume(ctx, outMany("out",
			docWeight{doc: add, w: -1},
			docWeight{doc: upsert, w: 1},
		))).To(Succeed())

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

	It("updater update patches metadata status and body from the pair diff", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

		old := map[string]any{
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

		scheme := kruntime.NewScheme()
		seed := keyObject(gvk, "default", "app")
		seed.Object = kruntime.DeepCopyJSON(old)

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

		Expect(u.Consume(ctx, outMany("out",
			docWeight{doc: old, w: -1},
			docWeight{doc: upsert, w: 1},
		))).To(Succeed())

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

	It("splits the pair diff between the main patch and the status subresource", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

		old := map[string]any{
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

		scheme := kruntime.NewScheme()
		seed := keyObject(gvk, "default", "app")
		seed.Object = kruntime.DeepCopyJSON(old)

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

		Expect(u.Consume(ctx, outMany("out",
			docWeight{doc: old, w: -1},
			docWeight{doc: upsert, w: 1},
		))).To(Succeed())

		// One main patch carrying only the spec change, one status patch
		// carrying only the status change.
		Expect(recording.patches).To(HaveLen(1))
		Expect(recording.patches[0]).To(HaveKeyWithValue("spec", map[string]any{"replicas": float64(2)}))
		Expect(recording.patches[0]).NotTo(HaveKey("status"))
		Expect(recording.statusPatches).To(HaveLen(1))
		Expect(recording.statusPatches[0]).To(HaveKeyWithValue("status", map[string]any{"availableReplicas": float64(2)}))
		Expect(recording.statusPatches[0]).NotTo(HaveKey("spec"))

		obj := keyObject(gvk, "default", "app")
		Expect(base.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		avail, ok, err := unstructured.NestedInt64(obj.Object, "status", "availableReplicas")
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

	It("patches view-object status inline through the main merge patch", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "test.view.dcontroller.io", Version: "v1alpha1", Kind: "HealthView"}

		scheme := kruntime.NewScheme()
		seed := keyObject(gvk, "default", "hv")
		seed.Object = map[string]any{
			"apiVersion": "test.view.dcontroller.io/v1alpha1",
			"kind":       "HealthView",
			"metadata":   map[string]any{"name": "hv", "namespace": "default"},
			"spec":       map[string]any{"target": "svc"},
		}

		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).Build()
		recording := &recordingClient{Client: base}

		p, err := NewPatcher(Config{Name: "test-patcher-view-status", Client: recording, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		doc := map[string]any{
			"apiVersion": "test.view.dcontroller.io/v1alpha1",
			"kind":       "HealthView",
			"metadata":   map[string]any{"name": "hv", "namespace": "default"},
			"status":     map[string]any{"healthy": true},
		}

		Expect(p.Consume(ctx, out("out", doc, 1))).To(Succeed())

		// Views have no status subresource: the status rides the main patch.
		Expect(recording.statusPatches).To(BeEmpty())
		Expect(recording.patches).To(HaveLen(1))
		Expect(recording.patches[0]).To(HaveKey("status"))

		obj := keyObject(gvk, "default", "hv")
		Expect(base.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		healthy, ok, err := unstructured.NestedBool(obj.Object, "status", "healthy")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(healthy).To(BeTrue())
	})

	It("patcher never creates: a write to a missing object is reported and dropped", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		p, err := NewPatcher(Config{Name: "test-patcher-no-create", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "ghost", "namespace": "default"},
			"data":       map[string]any{"a": "1"},
		}

		err = p.Consume(ctx, out("out", doc, 1))
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsNotFound(err) || err != nil).To(BeTrue())

		obj := keyObject(gvk, "default", "ghost")
		Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(obj), obj))).To(BeTrue(),
			"the patcher must not resurrect or create objects")
	})

	It("updater recreates an owned object deleted out of band", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		u, err := NewUpdater(Config{Name: "test-updater-resurrect", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		oldDoc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "gen", "namespace": "default"},
			"data":       map[string]any{"v": "1"},
		}
		newDoc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "gen", "namespace": "default"},
			"data":       map[string]any{"v": "2"},
		}

		// The update pair targets an object that is gone: the owner puts it
		// back with the desired content.
		Expect(u.Consume(ctx, outMany("out",
			docWeight{doc: oldDoc, w: -1},
			docWeight{doc: newDoc, w: 1},
		))).To(Succeed())

		obj := keyObject(gvk, "default", "gen")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		v, _, err := unstructured.NestedString(obj.Object, "data", "v")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("2"))
	})

	It("updater create falls back to a patch when the object already exists", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		scheme := kruntime.NewScheme()
		seed := keyObject(gvk, "default", "cfg")
		seed.Object = map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "cfg", "namespace": "default", "labels": map[string]any{"foreign": "yes"}},
			"data":       map[string]any{"a": "1"},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).Build()

		u, err := NewUpdater(Config{Name: "test-updater-exists", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "cfg", "namespace": "default"},
			"data":       map[string]any{"b": "2"},
		}

		Expect(u.Consume(ctx, out("out", doc, 1))).To(Succeed())

		obj := keyObject(gvk, "default", "cfg")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(obj), obj)).To(Succeed())
		b, _, err := unstructured.NestedString(obj.Object, "data", "b")
		Expect(err).NotTo(HaveOccurred())
		Expect(b).To(Equal("2"))
		// A bare assertion carries no old state, so it cannot remove
		// anything: fields it does not mention survive.
		a, _, err := unstructured.NestedString(obj.Object, "data", "a")
		Expect(err).NotTo(HaveOccurred())
		Expect(a).To(Equal("1"))
		Expect(obj.GetLabels()).To(HaveKeyWithValue("foreign", "yes"))
	})

	It("rejects key multiplicity instead of selecting a representative", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		scheme := kruntime.NewScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		u, err := NewUpdater(Config{Name: "test-updater-multiplicity", Client: c, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		d1 := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "dup", "namespace": "default"},
			"data":       map[string]any{"v": "1"},
		}
		d2 := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "dup", "namespace": "default"},
			"data":       map[string]any{"v": "2"},
		}

		err = u.Consume(ctx, outMany("out",
			docWeight{doc: d1, w: 1},
			docWeight{doc: d2, w: 1},
		))
		Expect(err).To(MatchError(ContainSubstring("2 asserted documents")))

		obj := keyObject(gvk, "default", "dup")
		Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(obj), obj))).To(BeTrue(),
			"neither candidate may be actuated")
	})

	It("retries unreachable writes until the plant answers", func() {
		ctx := context.Background()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		scheme := kruntime.NewScheme()
		base := fake.NewClientBuilder().WithScheme(scheme).Build()
		failing := &failingClient{Client: base, remaining: 1, err: apierrors.NewServiceUnavailable("plant down")}

		u, err := NewUpdater(Config{Name: "test-updater-retry", Client: failing, OutputName: "out", TargetGVK: gvk, Runtime: dbspruntime.NewRuntime(logr.Discard())})
		Expect(err).NotTo(HaveOccurred())

		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "late", "namespace": "default"},
			"data":       map[string]any{"a": "1"},
		}

		// The first attempt fails before the plant; the write stays pending
		// and the retry lands it. An unreachable failure is not an error:
		// the command is still owed to the plant.
		Expect(u.Consume(ctx, out("out", doc, 1))).To(Succeed())

		obj := keyObject(gvk, "default", "late")
		Expect(apierrors.IsNotFound(base.Get(ctx, client.ObjectKeyFromObject(obj), obj))).To(BeTrue())

		Eventually(func() error {
			o := keyObject(gvk, "default", "late")
			return base.Get(ctx, client.ObjectKeyFromObject(o), o)
		}, 5*time.Second, 100*time.Millisecond).Should(Succeed())
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

// recordingClient records the decoded bodies of main and status merge
// patches while forwarding to the wrapped client.
type recordingClient struct {
	client.Client
	patches       []map[string]any
	statusPatches []map[string]any
}

func (c *recordingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.patches = append(c.patches, decodePatch(patch, obj))
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *recordingClient) Status() client.StatusWriter {
	return &recordingStatusWriter{c: c}
}

type recordingStatusWriter struct {
	c *recordingClient
}

func (s *recordingStatusWriter) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	return s.c.Client.Status().Create(ctx, obj, sub, opts...)
}

func (s *recordingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return s.c.Client.Status().Update(ctx, obj, opts...)
}

func (s *recordingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	s.c.statusPatches = append(s.c.statusPatches, decodePatch(patch, obj))
	return s.c.Client.Status().Patch(ctx, obj, patch, opts...)
}

func (s *recordingStatusWriter) Apply(ctx context.Context, obj kruntime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return s.c.Client.Status().Apply(ctx, obj, opts...)
}

func decodePatch(patch client.Patch, obj client.Object) map[string]any {
	raw, err := patch.Data(obj)
	if err != nil {
		return nil
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

// failingClient fails every write with the given error until the remaining
// counter runs out, then forwards.
type failingClient struct {
	client.Client
	remaining int
	err       error
}

func (c *failingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.remaining > 0 {
		c.remaining--
		return c.err
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *failingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if c.remaining > 0 {
		c.remaining--
		return c.err
	}
	return c.Client.Create(ctx, obj, opts...)
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
