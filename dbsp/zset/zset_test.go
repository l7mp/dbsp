package zset

import (
	"testing"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/internal/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestZSet(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ZSet Suite")
}

var _ = Describe("ZSet", func() {
	Describe("New", func() {
		It("creates an empty Z-set", func() {
			z := New()
			Expect(z.IsZero()).To(BeTrue())
			Expect(z.Size()).To(Equal(0))
		})
	})

	Describe("Insert", func() {
		It("adds elements with positive weights", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 1)
			Expect(z.Size()).To(Equal(1))
			Expect(z.Lookup(testutils.StringElem("a").Hash())).To(Equal(Weight(1)))
		})

		It("sums weights for duplicate keys", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 1)
			z.Insert(testutils.StringElem("a"), 2)
			Expect(z.Size()).To(Equal(1))
			Expect(z.Lookup(testutils.StringElem("a").Hash())).To(Equal(Weight(3)))
		})

		It("removes elements when weight becomes zero", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 1)
			z.Insert(testutils.StringElem("a"), -1)
			Expect(z.IsZero()).To(BeTrue())
		})

		It("ignores zero-weight insertions", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 0)
			Expect(z.IsZero()).To(BeTrue())
		})

		It("supports negative weights", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), -2)
			Expect(z.Lookup(testutils.StringElem("a").Hash())).To(Equal(Weight(-2)))
		})
	})

	Describe("Lookup", func() {
		It("returns weight for existing element", func() {
			z := New()
			record := testutils.Record{ID: "1", Value: 10}
			z.Insert(record, 3)
			Expect(z.Lookup(record.Hash())).To(Equal(Weight(3)))
		})

		It("returns 0 for absent element", func() {
			z := New()
			Expect(z.Lookup(testutils.StringElem("missing").Hash())).To(Equal(Weight(0)))
		})
	})

	Describe("Elem", func() {
		It("returns element for existing key", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 5)
			v, ok := z.Elem("a")
			Expect(ok).To(BeTrue())
			Expect(v.Weight).To(Equal(Weight(5)))
		})

		It("returns false for absent key", func() {
			z := New()
			_, ok := z.Elem("a")
			Expect(ok).To(BeFalse())
		})
	})

	Describe("Iter", func() {
		It("iterates over all elements", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 1)
			z.Insert(testutils.StringElem("b"), 2)

			seen := make(map[string]Weight)
			z.Iter(func(elem datamodel.Document, weight Weight) bool {
				seen[string(elem.(testutils.StringElem))] = weight
				return true
			})

			Expect(seen).To(Equal(map[string]Weight{"a": 1, "b": 2}))
		})

		It("stops iteration when callback returns false", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 1)
			z.Insert(testutils.StringElem("b"), 2)
			z.Insert(testutils.StringElem("c"), 3)

			count := 0
			z.Iter(func(elem datamodel.Document, weight Weight) bool {
				count++
				return count < 2
			})

			Expect(count).To(Equal(2))
		})
	})

	Describe("Add", func() {
		It("combines two Z-sets", func() {
			a := New()
			a.Insert(testutils.StringElem("x"), 1)
			a.Insert(testutils.StringElem("y"), 2)

			b := New()
			b.Insert(testutils.StringElem("y"), 3)
			b.Insert(testutils.StringElem("z"), 4)

			sum := a.Add(b)

			Expect(sum.Lookup(testutils.StringElem("x").Hash())).To(Equal(Weight(1)))
			Expect(sum.Lookup(testutils.StringElem("y").Hash())).To(Equal(Weight(5)))
			Expect(sum.Lookup(testutils.StringElem("z").Hash())).To(Equal(Weight(4)))
		})

		It("does not modify originals", func() {
			a := New()
			a.Insert(testutils.StringElem("x"), 1)

			b := New()
			b.Insert(testutils.StringElem("x"), 2)

			_ = a.Add(b)

			Expect(a.Lookup(testutils.StringElem("x").Hash())).To(Equal(Weight(1)))
			Expect(b.Lookup(testutils.StringElem("x").Hash())).To(Equal(Weight(2)))
		})
	})

	Describe("Negate", func() {
		It("negates all weights", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 3)
			z.Insert(testutils.StringElem("b"), -2)

			neg := z.Negate()

			Expect(neg.Lookup(testutils.StringElem("a").Hash())).To(Equal(Weight(-3)))
			Expect(neg.Lookup(testutils.StringElem("b").Hash())).To(Equal(Weight(2)))
		})
	})

	Describe("Subtract", func() {
		It("computes difference", func() {
			a := New()
			a.Insert(testutils.StringElem("x"), 5)

			b := New()
			b.Insert(testutils.StringElem("x"), 2)

			diff := a.Subtract(b)
			Expect(diff.Lookup(testutils.StringElem("x").Hash())).To(Equal(Weight(3)))
		})
	})

	Describe("Scale", func() {
		It("multiplies all weights by a positive constant", func() {
			z := New()
			z.Insert(testutils.StringElem("x"), 3)
			z.Insert(testutils.StringElem("y"), 2)
			scaled := z.Scale(4)
			Expect(scaled.Lookup(testutils.StringElem("x").Hash())).To(Equal(Weight(12)))
			Expect(scaled.Lookup(testutils.StringElem("y").Hash())).To(Equal(Weight(8)))
		})

		It("negates when scaled by -1", func() {
			z := New()
			z.Insert(testutils.StringElem("x"), 5)
			Expect(z.Scale(-1).Lookup(testutils.StringElem("x").Hash())).To(Equal(Weight(-5)))
		})

		It("produces an empty Z-set when scaled by 0", func() {
			z := New()
			z.Insert(testutils.StringElem("x"), 5)
			Expect(z.Scale(0).Size()).To(Equal(0))
		})
	})

	Describe("Clone", func() {
		It("creates an independent copy", func() {
			z := New()
			z.Insert(testutils.StringElem("a"), 1)

			clone := z.Clone()
			clone.Insert(testutils.StringElem("a"), 5)

			Expect(z.Lookup(testutils.StringElem("a").Hash())).To(Equal(Weight(1)))
			Expect(clone.Lookup(testutils.StringElem("a").Hash())).To(Equal(Weight(6)))
		})
	})

	Describe("Equal", func() {
		It("returns true for equal Z-sets", func() {
			a := New()
			a.Insert(testutils.StringElem("x"), 1)
			a.Insert(testutils.StringElem("y"), 2)

			b := New()
			b.Insert(testutils.StringElem("y"), 2)
			b.Insert(testutils.StringElem("x"), 1)

			Expect(a.Equal(b)).To(BeTrue())
		})

		It("returns false for different sizes", func() {
			a := New()
			a.Insert(testutils.StringElem("x"), 1)

			b := New()

			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns false for different weights", func() {
			a := New()
			a.Insert(testutils.StringElem("x"), 1)

			b := New()
			b.Insert(testutils.StringElem("x"), 2)

			Expect(a.Equal(b)).To(BeFalse())
		})
	})
})
