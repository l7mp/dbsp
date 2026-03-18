package normalize_test

import (
	"encoding/base64"

	"github.com/l7mp/dbsp/datamodel/normalize"
	"github.com/l7mp/dbsp/datamodel/unstructured"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DecodeSecretData", func() {
	encode := func(s string) string {
		return base64.StdEncoding.EncodeToString([]byte(s))
	}

	It("decodes simple secret with multiple keys", func() {
		doc := unstructured.New(map[string]any{
			"kind": "Secret",
			"data": map[string]any{
				"username": encode("admin"),
				"password": encode("s3cr3t"),
				"config":   encode(`{"key":"value"}`),
			},
		}, nil)

		Expect(normalize.DecodeSecretData(doc)).To(Succeed())

		v, _ := doc.GetField("data.username")
		Expect(v).To(Equal("admin"))
		v, _ = doc.GetField("data.password")
		Expect(v).To(Equal("s3cr3t"))
		v, _ = doc.GetField("data.config")
		Expect(v).To(Equal(`{"key":"value"}`))
	})

	It("decodes binary payloads to raw strings", func() {
		doc := unstructured.New(map[string]any{
			"kind": "Secret",
			"data": map[string]any{
				"binarykey": base64.StdEncoding.EncodeToString([]byte{0x00, 0x01, 0x02, 0xFF}),
			},
		}, nil)

		Expect(normalize.DecodeSecretData(doc)).To(Succeed())
		v, _ := doc.GetField("data.binarykey")
		Expect(v).To(Equal("\x00\x01\x02\xff"))
	})

	It("keeps original value when decoding fails", func() {
		doc := unstructured.New(map[string]any{
			"kind": "Secret",
			"data": map[string]any{
				"username": "not-base64@@",
			},
		}, nil)

		Expect(normalize.DecodeSecretData(doc)).To(Succeed())
		v, _ := doc.GetField("data.username")
		Expect(v).To(Equal("not-base64@@"))
	})

	It("is no-op for non-secret documents", func() {
		doc := unstructured.New(map[string]any{
			"kind": "ConfigMap",
			"data": map[string]any{"key": "value"},
		}, nil)

		Expect(normalize.DecodeSecretData(doc)).To(Succeed())
		v, _ := doc.GetField("data.key")
		Expect(v).To(Equal("value"))
	})

	It("is safe on nil and malformed docs", func() {
		Expect(normalize.DecodeSecretData(nil)).To(Succeed())

		doc := unstructured.New(map[string]any{"kind": "Secret", "data": "not-a-map"}, nil)
		Expect(normalize.DecodeSecretData(doc)).To(Succeed())
	})
})
