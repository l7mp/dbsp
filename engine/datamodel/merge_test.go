package datamodel_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
)

var _ = Describe("CreateMergePatch", func() {
	It("writes changed fields and nulls vanished ones", func() {
		old := map[string]any{"a": "1", "b": "x"}
		new := map[string]any{"a": "2"}

		patch, err := datamodel.CreateMergePatch(old, new)
		Expect(err).NotTo(HaveOccurred())
		Expect(patch).To(Equal(map[string]any{"a": "2", "b": nil}))
	})

	It("returns an empty patch for equal documents", func() {
		old := map[string]any{"a": []any{1, 2}, "b": map[string]any{"c": true}}
		new := map[string]any{"a": []any{1, 2}, "b": map[string]any{"c": true}}

		patch, err := datamodel.CreateMergePatch(old, new)
		Expect(err).NotTo(HaveOccurred())
		Expect(patch).To(BeEmpty())
	})

	It("recurses into nested maps and emits only the changed leaves", func() {
		old := map[string]any{"m": map[string]any{"keep": 1, "change": 1, "drop": 1}}
		new := map[string]any{"m": map[string]any{"keep": 1, "change": 2, "add": 3}}

		patch, err := datamodel.CreateMergePatch(old, new)
		Expect(err).NotTo(HaveOccurred())
		Expect(patch).To(HaveLen(1))
		m := patch["m"].(map[string]any)
		Expect(m).To(HaveKeyWithValue("change", float64(2)))
		Expect(m).To(HaveKeyWithValue("add", float64(3)))
		Expect(m).To(HaveKey("drop"))
		Expect(m["drop"]).To(BeNil())
		Expect(m).NotTo(HaveKey("keep"))
	})

	It("treats arrays as atomic values", func() {
		old := map[string]any{"l": []any{1, 2, 3}}
		new := map[string]any{"l": []any{1, 2, 4}}

		patch, err := datamodel.CreateMergePatch(old, new)
		Expect(err).NotTo(HaveOccurred())
		Expect(patch["l"]).To(Equal([]any{float64(1), float64(2), float64(4)}))
	})

	It("compares numbers by value across Go types", func() {
		old := map[string]any{"n": int64(1)}
		new := map[string]any{"n": float64(1)}

		patch, err := datamodel.CreateMergePatch(old, new)
		Expect(err).NotTo(HaveOccurred())
		Expect(patch).To(BeEmpty())
	})

	It("removes a vanished map field by nulling its leaves", func() {
		old := map[string]any{"annotations": map[string]any{"a": "1", "b": map[string]any{"c": "2"}}}
		new := map[string]any{}

		patch, err := datamodel.CreateMergePatch(old, new)
		Expect(err).NotTo(HaveOccurred())
		m := patch["annotations"].(map[string]any)
		Expect(m).To(HaveKey("a"))
		Expect(m["a"]).To(BeNil())
		sub := m["b"].(map[string]any)
		Expect(sub).To(HaveKey("c"))
		Expect(sub["c"]).To(BeNil())
	})

	It("treats an explicit null in new as removal", func() {
		old := map[string]any{"a": 1}
		new := map[string]any{"a": nil}

		patch, err := datamodel.CreateMergePatch(old, new)
		Expect(err).NotTo(HaveOccurred())
		Expect(patch).To(Equal(map[string]any{"a": nil}))
	})

	It("elides a null for a field the old document lacks", func() {
		old := map[string]any{}
		new := map[string]any{"a": nil}

		patch, err := datamodel.CreateMergePatch(old, new)
		Expect(err).NotTo(HaveOccurred())
		Expect(patch).To(BeEmpty())
	})

	It("diffs against nil sides", func() {
		patch, err := datamodel.CreateMergePatch(nil, map[string]any{"a": 1})
		Expect(err).NotTo(HaveOccurred())
		Expect(patch).To(Equal(map[string]any{"a": float64(1)}))

		patch, err = datamodel.CreateMergePatch(map[string]any{"a": 1}, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(patch).To(Equal(map[string]any{"a": nil}))
	})
})
