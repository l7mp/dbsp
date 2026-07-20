package adaptor_test

import (
	"encoding/base64"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel/adaptor"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

func TestAdaptor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Adaptor Suite")
}

// upcase transforms string values under the given canonical path prefix.
func upcase(prefix string) adaptor.TransformFunc {
	return func(path string, v any) (any, error) {
		if !strings.HasPrefix(path, prefix) {
			return v, nil
		}
		if s, ok := v.(string); ok {
			return strings.ToUpper(s), nil
		}
		return v, nil
	}
}

var _ = Describe("SecretDataAdaptor", func() {
	It("decodes .data values on get and encodes on set", func() {
		doc := unstructured.New(map[string]any{"data": map[string]any{
			"username": base64.StdEncoding.EncodeToString([]byte("admin")),
		}})
		a := adaptor.SecretDataAdaptor(doc)

		v, err := a.GetField("$.data.username")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("admin"))

		Expect(a.SetField("$.data.password", "s3cr3t")).To(Succeed())
		raw, err := doc.GetField("$.data.password")
		Expect(err).NotTo(HaveOccurred())
		Expect(raw).To(Equal(base64.StdEncoding.EncodeToString([]byte("s3cr3t"))))
	})

	It("handles bracket paths for dotted data keys", func() {
		doc := unstructured.New(map[string]any{"data": map[string]any{
			"tls.crt": base64.StdEncoding.EncodeToString([]byte("pem")),
		}})
		a := adaptor.SecretDataAdaptor(doc)

		v, err := a.GetField(`$["data"]["tls.crt"]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("pem"))
	})

	It("leaves paths outside .data untouched", func() {
		doc := unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "web-cert"},
			"data":     map[string]any{},
		})
		a := adaptor.SecretDataAdaptor(doc)

		v, err := a.GetField("$.metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("web-cert"))
	})
})

var _ = Describe("Adaptor", func() {
	It("applies in transforms on get", func() {
		doc := unstructured.New(map[string]any{"data": map[string]any{"username": "admin"}})
		a := adaptor.New(doc, upcase("$.data."), nil)

		v, err := a.GetField("$.data.username")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("ADMIN"))

		// Paths outside the prefix pass through untouched.
		_, err = a.GetField("$.data")
		Expect(err).NotTo(HaveOccurred())
	})

	It("applies out transforms on set", func() {
		doc := unstructured.New(map[string]any{"data": map[string]any{}})
		a := adaptor.New(doc, nil, upcase("$.data."))

		Expect(a.SetField("$.data.password", "s3cr3t")).To(Succeed())
		v, err := doc.GetField("$.data.password")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("S3CR3T"))
	})

	It("supports wildcard nested matches", func() {
		doc := unstructured.New(map[string]any{"spec": map[string]any{"raw": int64(2)}})
		a := adaptor.New(doc,
			func(path string, v any) (any, error) {
				if path != "$.spec.raw" {
					return v, nil
				}
				if n, ok := v.(int64); ok {
					return n * 10, nil
				}
				return v, nil
			},
			nil,
		)

		v, err := a.GetField("$.spec.raw")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(20)))
	})

	It("supports chaining by wrapping adaptor on adaptor", func() {
		doc := unstructured.New(map[string]any{"data": map[string]any{"username": "admin"}})
		upper := adaptor.New(doc, upcase("$.data."), nil)
		bang := adaptor.New(upper, func(path string, v any) (any, error) {
			if path == "$.data.username" {
				if s, ok := v.(string); ok {
					return s + "!", nil
				}
			}
			return v, nil
		}, nil)

		v, err := bang.GetField("$.data.username")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("ADMIN!"))
	})
})
