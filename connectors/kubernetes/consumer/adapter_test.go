package consumer

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/engine/datamodel"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

// adapterFor builds a bare consumer for the adapter functions, which need
// no client and no runtime.
func adapterFor(gvk schema.GroupVersionKind) *baseConsumer {
	return &baseConsumer{targetGVK: gvk, converter: kobject.DefaultConverter}
}

var _ = Describe("Consumer adapters", func() {
	g := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

	It("adapts a document into the target object and its plant key", func() {
		doc := dbspunstructured.New(map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "demo",
				"namespace": "default",
			},
			"spec": map[string]any{"replicas": int64(3)},
		})

		key, obj, err := adapterFor(g).Adapt(doc)
		Expect(err).NotTo(HaveOccurred())
		Expect(key).To(Equal("default/demo"))

		target, ok := obj.(kobject.Object)
		Expect(ok).To(BeTrue())
		Expect(target.GetName()).To(Equal("demo"))
		Expect(target.GetNamespace()).To(Equal("default"))
		Expect(target.GroupVersionKind()).To(Equal(g))
	})

	It("stamps the target gvk on a document that carries another one", func() {
		doc := dbspunstructured.New(map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "n", "namespace": "ns"},
		})

		key, obj, err := adapterFor(g).Adapt(doc)
		Expect(err).NotTo(HaveOccurred())
		Expect(key).To(Equal("ns/n"))
		Expect(obj.(kobject.Object).GroupVersionKind()).To(Equal(g))
	})

	It("adapts a cluster-scoped document", func() {
		doc := dbspunstructured.New(map[string]any{"metadata": map[string]any{"name": "global"}})

		key, obj, err := adapterFor(g).Adapt(doc)
		Expect(err).NotTo(HaveOccurred())
		Expect(key).To(Equal("/global"), "an unnamespaced key keeps the empty namespace")
		Expect(obj.(kobject.Object).GetNamespace()).To(BeEmpty())
	})

	It("reports an unaddressable document instead of dropping it", func() {
		cases := map[string]datamodel.Document{
			"no metadata":     dbspunstructured.New(map[string]any{"spec": map[string]any{"a": int64(1)}}),
			"no name":         dbspunstructured.New(map[string]any{"metadata": map[string]any{"namespace": "ns"}}),
			"empty name":      dbspunstructured.New(map[string]any{"metadata": map[string]any{"name": ""}}),
			"non-string name": dbspunstructured.New(map[string]any{"metadata": map[string]any{"name": int64(7)}}),
			"bad namespace":   dbspunstructured.New(map[string]any{"metadata": map[string]any{"name": "n", "namespace": int64(2)}}),
		}

		for name, doc := range cases {
			_, _, err := adapterFor(g).Adapt(doc)
			Expect(err).To(HaveOccurred(), "case %q must be reported", name)
		}
	})
})
