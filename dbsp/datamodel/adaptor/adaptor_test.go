package adaptor_test

import (
	"encoding/base64"
	"testing"

	"github.com/l7mp/dbsp/dbsp/datamodel/adaptor"
	"github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAdaptor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Adaptor Suite")
}

var _ = Describe("Adaptor", func() {
	It("applies in transforms on get", func() {
		doc := unstructured.New(map[string]any{"data": map[string]any{"username": base64.StdEncoding.EncodeToString([]byte("admin"))}}, nil)
		a := adaptor.SecretDataAdaptor(doc)

		v, err := a.GetField("data.username")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("admin"))
	})

	It("applies out transforms on set", func() {
		doc := unstructured.New(map[string]any{"data": map[string]any{}}, nil)
		a := adaptor.SecretDataAdaptor(doc)

		Expect(a.SetField("data.password", "s3cr3t")).To(Succeed())
		v, err := doc.GetField("data.password")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(base64.StdEncoding.EncodeToString([]byte("s3cr3t"))))
	})

	It("supports wildcard nested matches", func() {
		doc := unstructured.New(map[string]any{"spec": map[string]any{"raw": int64(2)}}, nil)
		a := adaptor.New(doc,
			func(path string, v any) (any, error) {
				if path != "spec.raw" {
					return v, nil
				}
				if n, ok := v.(int64); ok {
					return n * 10, nil
				}
				return v, nil
			},
			nil,
		)

		v, err := a.GetField("spec.raw")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(20)))
	})

	It("supports chaining by wrapping adaptor on adaptor", func() {
		doc := unstructured.New(map[string]any{"data": map[string]any{"username": base64.StdEncoding.EncodeToString([]byte("admin"))}}, nil)
		secret := adaptor.SecretDataAdaptor(doc)
		upper := adaptor.New(secret, func(path string, v any) (any, error) {
			if path == "data.username" {
				if s, ok := v.(string); ok {
					return s + "!", nil
				}
			}
			return v, nil
		}, nil)

		v, err := upper.GetField("data.username")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("admin!"))
	})
})
