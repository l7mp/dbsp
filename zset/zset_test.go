package zset_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/zset"
)

func TestZSet(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ZSet Suite")
}

// Test element types.
type StringElem string

func (s StringElem) Key() any { return string(s) }

type Record struct {
	ID    string
	Value int
}

func (r Record) Key() any { return r.ID }

var _ = Describe("ZSet", func() {
	Describe("New", func() {
		It("creates an empty Z-set", func() {
			z := zset.New()
			Expect(z.IsZero()).To(BeTrue())
			Expect(z.Size()).To(Equal(0))
		})
	})

	Describe("Insert", func() {
		It("adds elements with positive weights", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 1)
			Expect(z.Size()).To(Equal(1))
			Expect(z.Lookup(StringElem("a"))).To(Equal(zset.Weight(1)))
		})

		It("sums weights for duplicate keys", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 1)
			z.Insert(StringElem("a"), 2)
			Expect(z.Size()).To(Equal(1))
			Expect(z.Lookup(StringElem("a"))).To(Equal(zset.Weight(3)))
		})

		It("removes elements when weight becomes zero", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 1)
			z.Insert(StringElem("a"), -1)
			Expect(z.IsZero()).To(BeTrue())
		})

		It("ignores zero-weight insertions", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 0)
			Expect(z.IsZero()).To(BeTrue())
		})

		It("supports negative weights", func() {
			z := zset.New()
			z.Insert(StringElem("a"), -2)
			Expect(z.Lookup(StringElem("a"))).To(Equal(zset.Weight(-2)))
		})
	})

	Describe("Lookup", func() {
		It("returns weight for existing element", func() {
			z := zset.New()
			z.Insert(Record{ID: "1", Value: 10}, 3)
			Expect(z.Lookup(Record{ID: "1", Value: 999})).To(Equal(zset.Weight(3)))
		})

		It("returns 0 for absent element", func() {
			z := zset.New()
			Expect(z.Lookup(StringElem("missing"))).To(Equal(zset.Weight(0)))
		})
	})

	Describe("LookupByKey", func() {
		It("returns weight for existing key", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 5)
			Expect(z.LookupByKey("a")).To(Equal(zset.Weight(5)))
		})

		It("returns 0 for absent key", func() {
			z := zset.New()
			Expect(z.LookupByKey("missing")).To(Equal(zset.Weight(0)))
		})
	})

	Describe("Iter", func() {
		It("iterates over all elements", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 1)
			z.Insert(StringElem("b"), 2)

			seen := make(map[string]zset.Weight)
			z.Iter(func(elem zset.Element, weight zset.Weight) bool {
				seen[string(elem.(StringElem))] = weight
				return true
			})

			Expect(seen).To(Equal(map[string]zset.Weight{"a": 1, "b": 2}))
		})

		It("stops iteration when callback returns false", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 1)
			z.Insert(StringElem("b"), 2)
			z.Insert(StringElem("c"), 3)

			count := 0
			z.Iter(func(elem zset.Element, weight zset.Weight) bool {
				count++
				return count < 2
			})

			Expect(count).To(Equal(2))
		})
	})

	Describe("Add", func() {
		It("combines two Z-sets", func() {
			a := zset.New()
			a.Insert(StringElem("x"), 1)
			a.Insert(StringElem("y"), 2)

			b := zset.New()
			b.Insert(StringElem("y"), 3)
			b.Insert(StringElem("z"), 4)

			sum := a.Add(b)

			Expect(sum.Lookup(StringElem("x"))).To(Equal(zset.Weight(1)))
			Expect(sum.Lookup(StringElem("y"))).To(Equal(zset.Weight(5)))
			Expect(sum.Lookup(StringElem("z"))).To(Equal(zset.Weight(4)))
		})

		It("does not modify originals", func() {
			a := zset.New()
			a.Insert(StringElem("x"), 1)

			b := zset.New()
			b.Insert(StringElem("x"), 2)

			_ = a.Add(b)

			Expect(a.Lookup(StringElem("x"))).To(Equal(zset.Weight(1)))
			Expect(b.Lookup(StringElem("x"))).To(Equal(zset.Weight(2)))
		})
	})

	Describe("Negate", func() {
		It("negates all weights", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 3)
			z.Insert(StringElem("b"), -2)

			neg := z.Negate()

			Expect(neg.Lookup(StringElem("a"))).To(Equal(zset.Weight(-3)))
			Expect(neg.Lookup(StringElem("b"))).To(Equal(zset.Weight(2)))
		})
	})

	Describe("Subtract", func() {
		It("computes difference", func() {
			a := zset.New()
			a.Insert(StringElem("x"), 5)

			b := zset.New()
			b.Insert(StringElem("x"), 2)

			diff := a.Subtract(b)
			Expect(diff.Lookup(StringElem("x"))).To(Equal(zset.Weight(3)))
		})
	})

	Describe("Clone", func() {
		It("creates an independent copy", func() {
			z := zset.New()
			z.Insert(StringElem("a"), 1)

			clone := z.Clone()
			clone.Insert(StringElem("a"), 5)

			Expect(z.Lookup(StringElem("a"))).To(Equal(zset.Weight(1)))
			Expect(clone.Lookup(StringElem("a"))).To(Equal(zset.Weight(6)))
		})
	})

	Describe("Equal", func() {
		It("returns true for equal Z-sets", func() {
			a := zset.New()
			a.Insert(StringElem("x"), 1)
			a.Insert(StringElem("y"), 2)

			b := zset.New()
			b.Insert(StringElem("y"), 2)
			b.Insert(StringElem("x"), 1)

			Expect(a.Equal(b)).To(BeTrue())
		})

		It("returns false for different sizes", func() {
			a := zset.New()
			a.Insert(StringElem("x"), 1)

			b := zset.New()

			Expect(a.Equal(b)).To(BeFalse())
		})

		It("returns false for different weights", func() {
			a := zset.New()
			a.Insert(StringElem("x"), 1)

			b := zset.New()
			b.Insert(StringElem("x"), 2)

			Expect(a.Equal(b)).To(BeFalse())
		})
	})
})
