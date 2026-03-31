package producer

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Producer adapters", func() {
	It("converts add/update/delete lifecycle to zset deltas", func() {
		p := &baseProducer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

		obj := kobject.New()
		gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace("default")
		obj.SetName("app")
		kobject.SetContent(obj, map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{"replicas": int64(1)},
		})

		zs, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.Size()).To(Equal(1))

		updated := kobject.DeepCopy(obj)
		kobject.SetContent(updated, map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "app",
				"namespace": "default",
			},
			"spec": map[string]any{"replicas": int64(2)},
		})

		zs, err = p.convertDeltaToZSet(kobject.Delta{Type: kobject.Updated, Object: updated})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.Size()).To(Equal(2))

		zs, err = p.convertDeltaToZSet(kobject.Delta{Type: kobject.Deleted, Object: updated})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.Size()).To(Equal(1))
	})

	It("suppresses noop updates", func() {
		p := &baseProducer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

		obj := kobject.New()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace("default")
		obj.SetName("cfg")
		kobject.SetContent(obj, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "cfg",
				"namespace": "default",
			},
			"data": map[string]any{"a": "1"},
		})

		_, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
		Expect(err).NotTo(HaveOccurred())

		zs, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Updated, Object: kobject.DeepCopy(obj)})
		Expect(err).NotTo(HaveOccurred())
		Expect(zs.IsZero()).To(BeTrue())
	})

	It("uses cached object on delete tombstones", func() {
		p := &baseProducer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

		obj := kobject.New()
		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace("default")
		obj.SetName("cfg")
		kobject.SetContent(obj, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":            "cfg",
				"namespace":       "default",
				"resourceVersion": "10",
			},
			"data": map[string]any{"a": "1"},
		})

		zsAdd, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
		Expect(err).NotTo(HaveOccurred())
		Expect(zsAdd.Size()).To(Equal(1))

		tombstone := kobject.DeepCopy(obj)
		kobject.SetContent(tombstone, map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":            "cfg",
				"namespace":       "default",
				"resourceVersion": "11",
			},
			"data": map[string]any{"a": "1"},
		})

		zsDel, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Deleted, Object: tombstone})
		Expect(err).NotTo(HaveOccurred())
		Expect(zsDel.Size()).To(Equal(1))

		combined := zsAdd.Add(zsDel)
		Expect(combined.IsZero()).To(BeTrue())
	})
})
