package product_test

import (
	"fmt"
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
			"pod": unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}, nil),
			"dep": unstructured.New(map[string]any{"metadata": map[string]any{"name": "d1"}}, nil),
		})

		v, err := p.GetField("pod.metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("p1"))

		Expect(p.SetField("dep.metadata.name", "d2")).To(Succeed())
		v, err = p.GetField("dep.metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("d2"))
	})

	It("builds primary key from part primary keys", func() {
		p := product.New(map[string]datamodel.Document{
			"pod": unstructured.New(map[string]any{
				"metadata": map[string]any{"namespace": "default", "name": "p1"},
			}, func(doc datamodel.Document) (string, error) {
				ns, err := doc.GetField("metadata.namespace")
				if err != nil {
					return "", err
				}
				name, err := doc.GetField("metadata.name")
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("%s/%s", ns, name), nil
			}),
			"dep": unstructured.New(map[string]any{
				"metadata": map[string]any{"namespace": "default", "name": "d1"},
			}, func(doc datamodel.Document) (string, error) {
				ns, err := doc.GetField("metadata.namespace")
				if err != nil {
					return "", err
				}
				name, err := doc.GetField("metadata.name")
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("%s/%s", ns, name), nil
			}),
		})

		pk, err := p.PrimaryKey()
		Expect(err).NotTo(HaveOccurred())
		Expect(pk).To(Equal("default/d1:default/p1"))
	})

	It("is associative across nested products", func() {
		dep := unstructured.New(map[string]any{"metadata": map[string]any{"name": "d1"}}, func(doc datamodel.Document) (string, error) {
			name, err := doc.GetField("metadata.name")
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("dep:%s", name), nil
		})
		pod := unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}, func(doc datamodel.Document) (string, error) {
			name, err := doc.GetField("metadata.name")
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("pod:%s", name), nil
		})
		rs := unstructured.New(map[string]any{"metadata": map[string]any{"name": "r1"}}, func(doc datamodel.Document) (string, error) {
			name, err := doc.GetField("metadata.name")
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("rs:%s", name), nil
		})

		leftAssoc := product.New(map[string]datamodel.Document{
			"x":  product.New(map[string]datamodel.Document{"dep": dep, "pod": pod}),
			"rs": rs,
		})
		rightAssoc := product.New(map[string]datamodel.Document{
			"dep": dep,
			"y":   product.New(map[string]datamodel.Document{"pod": pod, "rs": rs}),
		})

		pkLeft, err := leftAssoc.PrimaryKey()
		Expect(err).NotTo(HaveOccurred())
		pkRight, err := rightAssoc.PrimaryKey()
		Expect(err).NotTo(HaveOccurred())

		Expect(pkLeft).To(Equal(pkRight))
		Expect(pkLeft).To(Equal("dep:d1:pod:p1:rs:r1"))
	})

	It("concatenates nested part primary keys", func() {
		nested := product.New(map[string]datamodel.Document{
			"pod": unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}, func(doc datamodel.Document) (string, error) {
				name, err := doc.GetField("metadata.name")
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("%s", name), nil
			}),
		})
		p := product.New(map[string]datamodel.Document{
			"dep": unstructured.New(map[string]any{"metadata": map[string]any{"name": "d1"}}, func(doc datamodel.Document) (string, error) {
				name, err := doc.GetField("metadata.name")
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("%s", name), nil
			}),
			"ctx": nested,
		})

		pk, err := p.PrimaryKey()
		Expect(err).NotTo(HaveOccurred())
		Expect(pk).To(Equal("d1:p1"))
	})

	It("returns error when part primary key is unavailable", func() {
		p := product.New(map[string]datamodel.Document{
			"ok": unstructured.New(map[string]any{"metadata": map[string]any{"name": "ok"}}, nil),
			"bad": unstructured.New(map[string]any{"metadata": map[string]any{"name": "bad"}}, func(doc datamodel.Document) (string, error) {
				return "", fmt.Errorf("no pk")
			}),
		})

		_, err := p.PrimaryKey()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`part "bad"`))
	})
})
