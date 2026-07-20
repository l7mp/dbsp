package product_test

import (
	"testing"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/product"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestProduct(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Product Suite")
}

var _ = Describe("Product", func() {
	It("supports nested get/set", func() {
		p := product.New(map[string]datamodel.Document{
			"pod": unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}),
			"dep": unstructured.New(map[string]any{"metadata": map[string]any{"name": "d1"}}),
		})

		v, err := p.GetField("$.pod.metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("p1"))

		Expect(p.SetField("$.dep.metadata.name", "d2")).To(Succeed())
		v, err = p.GetField("$.dep.metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("d2"))
	})

	It("preserves nil namespaces", func() {
		p := product.New(map[string]datamodel.Document{
			"pod": unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}),
			"svc": nil,
		})

		svc, err := p.GetField("$.svc")
		Expect(err).NotTo(HaveOccurred())
		Expect(svc).To(BeNil())

		_, err = p.GetField("$.svc.metadata.name")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("field not found"))
	})
})
