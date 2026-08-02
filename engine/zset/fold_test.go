package zset

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

// byName keys documents on their top-level "name" field; documents without
// one are unkeyed.
func byName(doc datamodel.Document) (string, error) {
	v, err := doc.GetField("$.name")
	if err != nil {
		return "", nil //nolint:nilerr // a missing name means the document is unkeyed
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("name is not a string")
	}
	return s, nil
}

func doc(fields map[string]any) datamodel.Document {
	return unstructured.New(fields)
}

var _ = Describe("Fold", func() {
	It("pairs a retraction and an assertion for the same key", func() {
		z := New()
		z.Insert(doc(map[string]any{"name": "a", "v": 1}), -1)
		z.Insert(doc(map[string]any{"name": "a", "v": 2}), 1)

		pairs, errs := Fold(z, byName)
		Expect(errs).To(BeEmpty())
		Expect(pairs).To(HaveLen(1))
		Expect(pairs[0].Key).To(Equal("a"))
		Expect(pairs[0].Old).NotTo(BeNil())
		Expect(pairs[0].New).NotTo(BeNil())
		Expect(pairs[0].Old.Fields()["v"]).To(BeEquivalentTo(1))
		Expect(pairs[0].New.Fields()["v"]).To(BeEquivalentTo(2))
	})

	It("folds a bare assertion into a create-shaped pair", func() {
		z := New()
		z.Insert(doc(map[string]any{"name": "a", "v": 1}), 1)

		pairs, errs := Fold(z, byName)
		Expect(errs).To(BeEmpty())
		Expect(pairs).To(HaveLen(1))
		Expect(pairs[0].Old).To(BeNil())
		Expect(pairs[0].New).NotTo(BeNil())
	})

	It("folds a bare retraction into a delete-shaped pair", func() {
		z := New()
		z.Insert(doc(map[string]any{"name": "a", "v": 1}), -1)

		pairs, errs := Fold(z, byName)
		Expect(errs).To(BeEmpty())
		Expect(pairs).To(HaveLen(1))
		Expect(pairs[0].Old).NotTo(BeNil())
		Expect(pairs[0].New).To(BeNil())
	})

	It("returns pairs in deterministic key order", func() {
		z := New()
		z.Insert(doc(map[string]any{"name": "b"}), 1)
		z.Insert(doc(map[string]any{"name": "a"}), 1)
		z.Insert(doc(map[string]any{"name": "c"}), 1)

		pairs, errs := Fold(z, byName)
		Expect(errs).To(BeEmpty())
		Expect([]string{pairs[0].Key, pairs[1].Key, pairs[2].Key}).
			To(Equal([]string{"a", "b", "c"}))
	})

	It("rejects two asserted documents for one key", func() {
		z := New()
		z.Insert(doc(map[string]any{"name": "a", "v": 1}), 1)
		z.Insert(doc(map[string]any{"name": "a", "v": 2}), 1)

		pairs, errs := Fold(z, byName)
		Expect(pairs).To(BeEmpty())
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Key).To(Equal("a"))
		Expect(errs[0].Error()).To(ContainSubstring("2 asserted documents"))
	})

	It("rejects non-unit weights", func() {
		z := New()
		z.Insert(doc(map[string]any{"name": "a", "v": 1}), 2)

		pairs, errs := Fold(z, byName)
		Expect(pairs).To(BeEmpty())
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Error()).To(ContainSubstring("weight 2"))
	})

	It("rejects one key without blocking the others", func() {
		z := New()
		z.Insert(doc(map[string]any{"name": "a", "v": 1}), 1)
		z.Insert(doc(map[string]any{"name": "a", "v": 2}), 1)
		z.Insert(doc(map[string]any{"name": "b", "v": 1}), 1)

		pairs, errs := Fold(z, byName)
		Expect(pairs).To(HaveLen(1))
		Expect(pairs[0].Key).To(Equal("b"))
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Key).To(Equal("a"))
	})

	It("skips unkeyed documents", func() {
		z := New()
		z.Insert(doc(map[string]any{"v": 1}), 1)

		pairs, errs := Fold(z, byName)
		Expect(pairs).To(BeEmpty())
		Expect(errs).To(BeEmpty())
	})

	It("reports key-function errors per document", func() {
		z := New()
		z.Insert(doc(map[string]any{"name": 42}), 1)

		pairs, errs := Fold(z, byName)
		Expect(pairs).To(BeEmpty())
		Expect(errs).To(HaveLen(1))
		Expect(errs[0].Key).To(Equal(""))
	})

	It("nets out identical documents inside the Z-set", func() {
		z := New()
		d := map[string]any{"name": "a", "v": 1}
		z.Insert(doc(d), 1)
		z.Insert(doc(d), -1)

		pairs, errs := Fold(z, byName)
		Expect(errs).To(BeEmpty())
		Expect(pairs).To(BeEmpty())
	})
})
