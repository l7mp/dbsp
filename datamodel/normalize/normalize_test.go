package normalize_test

import (
	"encoding/base64"
	"testing"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/datamodel/normalize"
	"github.com/l7mp/dbsp/datamodel/unstructured"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestNormalize(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Normalize Suite")
}

var _ = Describe("Normalize Chain", func() {
	It("returns nil for nil input", func() {
		chain := normalize.NewChain(normalize.TransformerFunc(normalize.DecodeSecretData))
		out, err := chain.Normalize(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(BeNil())
	})

	It("applies transformations on a copy", func() {
		orig := unstructured.New(map[string]any{
			"kind": "Secret",
			"data": map[string]any{
				"username": base64.StdEncoding.EncodeToString([]byte("admin")),
			},
		}, nil)

		chain := normalize.NewChain(normalize.TransformerFunc(normalize.DecodeSecretData))
		out, err := chain.Normalize(orig)
		Expect(err).NotTo(HaveOccurred())
		Expect(out.Hash()).NotTo(Equal(orig.Hash()))

		v, err := out.GetField("data.username")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("admin"))

		origV, err := orig.GetField("data.username")
		Expect(err).NotTo(HaveOccurred())
		Expect(origV).NotTo(Equal("admin"))
	})

	It("returns errors from failing transforms", func() {
		chain := normalize.NewChain(normalize.TransformerFunc(func(datamodel.Document) error {
			return datamodel.ErrFieldNotFound
		}))
		_, err := chain.Normalize(unstructured.New(map[string]any{"a": int64(1)}, nil))
		Expect(err).To(HaveOccurred())
	})
})
