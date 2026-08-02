package consumer

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

var _ = Describe("Consumer adapters", func() {
	It("normalizes result object metadata and gvk", func() {
		g := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

		input := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "demo",
				"namespace": "default",
			},
			"spec": map[string]any{"replicas": int64(3)},
		}

		obj, err := kobject.DefaultConverter.ToObject(dbspunstructured.New(input))
		Expect(err).NotTo(HaveOccurred())

		out := normalizeResultObject(obj, g)
		Expect(out).NotTo(BeNil())
		Expect(out.GetName()).To(Equal("demo"))
		Expect(out.GetNamespace()).To(Equal("default"))
		Expect(out.GroupVersionKind()).To(Equal(g))
	})

	It("returns nil for invalid metadata", func() {
		g := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
		obj, err := kobject.DefaultConverter.ToObject(dbspunstructured.New(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "spec": map[string]any{"a": 1}}))
		Expect(err).NotTo(HaveOccurred())

		Expect(normalizeResultObject(obj, g)).To(BeNil())
	})

	It("keys documents on the target identity", func() {
		doc := dbspunstructured.New(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "n", "namespace": "ns"}})

		bc := &baseConsumer{
			targetGVK: schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "K"},
			converter: kobject.DefaultConverter,
		}
		key, err := bc.keyOf(doc)
		Expect(err).NotTo(HaveOccurred())
		Expect(key).To(Equal("ns/n"))
	})
})
