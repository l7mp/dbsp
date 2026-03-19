package unstructured_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
)

func TestUnstructured(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Unstructured Suite")
}

var _ = Describe("Unstructured", func() {
	Describe("Copy", func() {
		It("deep copies a flat document", func() {
			orig := unstructured.New(map[string]any{"a": int64(1), "b": "hello"}, nil)
			cp := orig.Copy()
			Expect(cp.Hash()).To(Equal(orig.Hash()))

			// Mutating the copy must not affect the original.
			Expect(cp.SetField("a", int64(99))).To(Succeed())
			Expect(orig.Hash()).To(Equal(unstructured.New(map[string]any{"a": int64(1), "b": "hello"}, nil).Hash()))
		})

		It("deep copies nested maps", func() {
			inner := map[string]any{"x": int64(1)}
			orig := unstructured.New(map[string]any{"nested": inner}, nil)
			cp := orig.Copy()

			// Mutate the nested map inside the copy via GetField.
			nested, err := cp.GetField("nested")
			Expect(err).NotTo(HaveOccurred())
			nestedMap, ok := nested.(map[string]any)
			Expect(ok).To(BeTrue())
			nestedMap["x"] = int64(42)

			// Original must be unaffected.
			origNested, err := orig.GetField("nested")
			Expect(err).NotTo(HaveOccurred())
			Expect(origNested.(map[string]any)["x"]).To(Equal(int64(1)))
		})

		It("deep copies slices", func() {
			orig := unstructured.New(map[string]any{"tags": []any{"a", "b"}}, nil)
			cp := orig.Copy()

			// Mutate the slice inside the copy.
			tags, err := cp.GetField("tags")
			Expect(err).NotTo(HaveOccurred())
			tagSlice, ok := tags.([]any)
			Expect(ok).To(BeTrue())
			tagSlice[0] = "z"

			// Original must be unaffected.
			origTags, err := orig.GetField("tags")
			Expect(err).NotTo(HaveOccurred())
			Expect(origTags.([]any)[0]).To(Equal("a"))
		})

		It("deep copies three levels of nesting", func() {
			deep := map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"level3": "original",
					},
				},
			}
			orig := unstructured.New(deep, nil)
			cp := orig.Copy()

			// Drill into the copy and mutate the deepest level.
			l1, _ := cp.GetField("level1")
			l2, _ := l1.(map[string]any)["level2"].(map[string]any)
			l2["level3"] = "mutated"

			// Original must be unaffected.
			origL1, _ := orig.GetField("level1")
			origL2 := origL1.(map[string]any)["level2"].(map[string]any)
			Expect(origL2["level3"]).To(Equal("original"))
		})
	})

	Describe("New", func() {
		It("does not alias the caller's nested map", func() {
			inner := map[string]any{"k": "orig"}
			source := map[string]any{"sub": inner}
			doc := unstructured.New(source, nil)

			// Mutate the caller's original map.
			inner["k"] = "mutated"

			// Document must be unaffected.
			sub, err := doc.GetField("sub")
			Expect(err).NotTo(HaveOccurred())
			Expect(sub.(map[string]any)["k"]).To(Equal("orig"))
		})
	})
})
