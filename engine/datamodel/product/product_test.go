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

	It("resolves every spelling of a member path identically", func() {
		p := product.New(map[string]datamodel.Document{
			"pod": unstructured.New(map[string]any{
				"metadata": map[string]any{"name": "p1"},
				"spec": map[string]any{
					"containers": []any{map[string]any{"image": "img"}},
				},
			}),
		})

		for _, path := range []string{
			"$.pod.metadata.name",
			`$.pod["metadata"]["name"]`,
			`$["pod"]["metadata"]["name"]`,
			`$["pod"].metadata.name`,
		} {
			v, err := p.GetField(path)
			Expect(err).NotTo(HaveOccurred(), path)
			Expect(v).To(Equal("p1"), path)
		}

		v, err := p.GetField("$.pod.spec.containers[0].image")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("img"))
	})

	It("returns the member document for any member-path spelling", func() {
		p := product.New(map[string]datamodel.Document{
			"pod": unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}),
		})

		dotted, err := p.GetField("$.pod")
		Expect(err).NotTo(HaveOccurred())
		bracket, err := p.GetField(`$["pod"]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(bracket).To(Equal(dotted))
		Expect(bracket).To(BeAssignableToTypeOf(&unstructured.Unstructured{}))
	})

	It("delegates dotted map keys in the remainder path", func() {
		p := product.New(map[string]datamodel.Document{
			"secret": unstructured.New(map[string]any{
				"data": map[string]any{"tls.crt": "PEM"},
			}),
		})

		v, err := p.GetField(`$.secret["data"]["tls.crt"]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("PEM"))
	})

	It("sets through any member-path spelling", func() {
		p := product.New(map[string]datamodel.Document{
			"pod": unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}),
		})

		Expect(p.SetField(`$.pod["metadata"]["name"]`, "p2")).To(Succeed())
		v, err := p.GetField("$.pod.metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("p2"))
	})

	It("evaluates non-child leading fragments against the whole product", func() {
		p := product.New(map[string]datamodel.Document{
			"pod": unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}),
		})

		v, err := p.GetField("$[*]")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(map[string]any{"metadata": map[string]any{"name": "p1"}}))
	})
})
