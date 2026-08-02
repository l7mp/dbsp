package datamodel_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
)

func TestDatamodel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Datamodel Suite")
}

var _ = Describe("DeepCopyAny", func() {
	It("clones maps and slices recursively", func() {
		orig := map[string]any{"m": map[string]any{"k": []any{1, 2}}}
		cp := datamodel.DeepCopyAny(orig).(map[string]any)
		cp["m"].(map[string]any)["k"].([]any)[0] = 99
		Expect(orig["m"].(map[string]any)["k"].([]any)[0]).To(Equal(1))
	})

	It("returns primitives as-is", func() {
		Expect(datamodel.DeepCopyAny("s")).To(Equal("s"))
		Expect(datamodel.DeepCopyAny(nil)).To(BeNil())
	})
})

var _ = Describe("NormalizeAny", func() {
	It("lands every number as float64", func() {
		out, err := datamodel.NormalizeAny(map[string]any{"n": int64(1)})
		Expect(err).NotTo(HaveOccurred())
		Expect(out.(map[string]any)["n"]).To(Equal(float64(1)))
	})

	It("normalizes values with equal canonical JSON to deep-equal results", func() {
		a, err := datamodel.NormalizeAny(map[string]any{"n": int64(1), "l": []any{int64(2)}})
		Expect(err).NotTo(HaveOccurred())
		b, err := datamodel.NormalizeAny(map[string]any{"n": float64(1), "l": []any{float64(2)}})
		Expect(err).NotTo(HaveOccurred())
		Expect(a).To(Equal(b))
	})
})

var _ = Describe("Normalize", func() {
	It("normalizes a nil map to an empty one", func() {
		out, err := datamodel.Normalize(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(BeEmpty())
	})

	It("shares no memory with the input", func() {
		in := map[string]any{"m": map[string]any{"k": "v"}}
		out, err := datamodel.Normalize(in)
		Expect(err).NotTo(HaveOccurred())
		out["m"].(map[string]any)["k"] = "w"
		Expect(in["m"].(map[string]any)["k"]).To(Equal("v"))
	})
})
