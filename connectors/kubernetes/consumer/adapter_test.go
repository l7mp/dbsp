package consumer

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
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

		obj, err := toObject(dbunstructured.New(input, nil))
		Expect(err).NotTo(HaveOccurred())

		out := normalizeResultObject(obj, g)
		Expect(out).NotTo(BeNil())
		Expect(out.GetName()).To(Equal("demo"))
		Expect(out.GetNamespace()).To(Equal("default"))
		Expect(out.GroupVersionKind()).To(Equal(g))
	})

	It("returns nil for invalid metadata", func() {
		g := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
		obj, err := toObject(dbunstructured.New(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "spec": map[string]any{"a": 1}}, nil))
		Expect(err).NotTo(HaveOccurred())

		Expect(normalizeResultObject(obj, g)).To(BeNil())
	})

	It("marks negative weights as delete", func() {
		doc := dbunstructured.New(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "n"}}, nil)
		e := zset.Elem{Document: doc, Weight: -1}

		bc := &baseConsumer{targetGVK: schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "K"}}
		obj, isDelete, err := bc.objectFromElem(e)
		Expect(err).NotTo(HaveOccurred())
		Expect(obj).NotTo(BeNil())
		Expect(isDelete).To(BeTrue())
	})
})
