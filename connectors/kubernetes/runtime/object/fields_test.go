package object

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("API fields", func() {
	native := func() map[string]any {
		return map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":              "cfg",
				"namespace":         "default",
				"uid":               "1234",
				"resourceVersion":   "219",
				"generation":        int64(7),
				"creationTimestamp": "2026-01-01T00:00:00Z",
				"deletionTimestamp": "2026-01-02T00:00:00Z",
				"selfLink":          "/api/v1/namespaces/default/configmaps/cfg",
				"managedFields":     []any{map[string]any{"manager": "kubectl"}},
				"annotations": map[string]any{
					LastAppliedConfigAnnotation: "{}",
					"keep.me":                   "yes",
				},
			},
			"data": map[string]any{"a": "1"},
		}
	}

	Describe("StripOnIngest", func() {
		It("drops what carries no information and keeps what does", func() {
			content := native()
			StripOnIngest(content)

			meta := content["metadata"].(map[string]any)
			Expect(meta).NotTo(HaveKey("resourceVersion"))
			Expect(meta).NotTo(HaveKey("managedFields"))
			Expect(meta).NotTo(HaveKey("selfLink"))
			Expect(meta).To(HaveKeyWithValue("uid", "1234"))
			Expect(meta).To(HaveKeyWithValue("generation", int64(7)))
			Expect(meta).To(HaveKey("creationTimestamp"))
			Expect(meta).To(HaveKey("deletionTimestamp"))
			Expect(content).To(HaveKey("data"))

			anns := meta["annotations"].(map[string]any)
			Expect(anns).NotTo(HaveKey(LastAppliedConfigAnnotation))
			Expect(anns).To(HaveKeyWithValue("keep.me", "yes"))
		})

		It("drops the UID of view objects, whose UID is derived from identity", func() {
			content := native()
			content["apiVersion"] = "view.dcontroller.io/v1alpha1"
			content["kind"] = "MyView"

			StripOnIngest(content)
			Expect(content["metadata"].(map[string]any)).NotTo(HaveKey("uid"))
		})

		It("removes an annotation map left empty by the stripping", func() {
			content := native()
			content["metadata"].(map[string]any)["annotations"] = map[string]any{
				LastAppliedConfigAnnotation: "{}",
			}

			StripOnIngest(content)
			Expect(content["metadata"].(map[string]any)).NotTo(HaveKey("annotations"))
		})

		It("makes an object differing only in API fields compare equal", func() {
			a, b := native(), native()
			b["metadata"].(map[string]any)["resourceVersion"] = "220"
			b["metadata"].(map[string]any)["managedFields"] = []any{map[string]any{"manager": "other"}}

			StripOnIngest(a)
			StripOnIngest(b)
			Expect(a).To(Equal(b))
		})
	})

	Describe("StripOnWrite", func() {
		It("removes every API field, including the ones ingest keeps", func() {
			content := native()
			StripOnWrite(content)

			meta := content["metadata"].(map[string]any)
			Expect(meta).To(HaveKeyWithValue("name", "cfg"))
			Expect(meta).To(HaveKeyWithValue("namespace", "default"))
			for _, f := range []string{
				"uid", "resourceVersion", "generation", "creationTimestamp",
				"deletionTimestamp", "selfLink", "managedFields",
			} {
				Expect(meta).NotTo(HaveKey(f))
			}
			Expect(content).To(HaveKey("data"))
		})
	})
})
