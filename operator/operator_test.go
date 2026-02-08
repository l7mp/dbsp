package operator_test

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/operator"
	"github.com/l7mp/dbsp/zset"
)

func TestOperator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Operator Suite")
}

// Test element types.
type Record struct {
	ID    string
	Value int
}

func (r Record) Key() string { return r.ID }

type StringElem string

func (s StringElem) Key() string { return string(s) }

func zsetOf(elem zset.Element, weight zset.Weight) zset.ZSet {
	z := zset.New()
	z.Insert(elem, weight)
	return z
}

var _ = Describe("Operators", func() {
	Describe("Linearity", func() {
		It("has correct string representation", func() {
			Expect(operator.Linear.String()).To(Equal("Linear"))
			Expect(operator.Bilinear.String()).To(Equal("Bilinear"))
			Expect(operator.NonLinear.String()).To(Equal("NonLinear"))
		})
	})

	Describe("Negate", func() {
		It("negates all weights", func() {
			op := operator.NewNegate()
			Expect(op.Name()).To(Equal("negate"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(operator.Linear))

			input := zset.New()
			input.Insert(Record{ID: "a", Value: 1}, 3)
			input.Insert(Record{ID: "b", Value: 2}, -2)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(Record{ID: "a"})).To(Equal(zset.Weight(-3)))
			Expect(result.Lookup(Record{ID: "b"})).To(Equal(zset.Weight(2)))
		})
	})

	Describe("Plus", func() {
		It("adds two Z-sets", func() {
			op := operator.NewPlus()
			Expect(op.Name()).To(Equal("plus"))
			Expect(op.Arity()).To(Equal(2))
			Expect(op.Linearity()).To(Equal(operator.Linear))

			a := zset.New()
			a.Insert(Record{ID: "x", Value: 1}, 1)

			b := zset.New()
			b.Insert(Record{ID: "x", Value: 1}, 2)
			b.Insert(Record{ID: "y", Value: 2}, 3)

			result, err := op.Apply(a, b)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(Record{ID: "x"})).To(Equal(zset.Weight(3)))
			Expect(result.Lookup(Record{ID: "y"})).To(Equal(zset.Weight(3)))
		})
	})

	Describe("Select", func() {
		It("filters elements by predicate", func() {
			predicate := expr.Func(func(e zset.Element) (any, error) {
				return e.(Record).Value > 5, nil
			})
			op := operator.NewSelect("gt5", predicate)
			Expect(op.Name()).To(Equal("gt5"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(operator.Linear))

			input := zset.New()
			input.Insert(Record{ID: "a", Value: 3}, 1)
			input.Insert(Record{ID: "b", Value: 7}, 2)
			input.Insert(Record{ID: "c", Value: 10}, 3)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(2))
			Expect(result.Lookup(Record{ID: "b"})).To(Equal(zset.Weight(2)))
			Expect(result.Lookup(Record{ID: "c"})).To(Equal(zset.Weight(3)))
		})

		It("preserves weights", func() {
			predicate := expr.Func(func(e zset.Element) (any, error) {
				return true, nil
			})
			op := operator.NewSelect("all", predicate)

			input := zset.New()
			input.Insert(Record{ID: "a", Value: 1}, 5)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(Record{ID: "a"})).To(Equal(zset.Weight(5)))
		})
	})

	Describe("Project", func() {
		It("transforms elements", func() {
			projection := expr.Func(func(e zset.Element) (any, error) {
				r := e.(Record)
				return Record{ID: r.ID, Value: r.Value * 2}, nil
			})
			op := operator.NewProject("double", projection)
			Expect(op.Name()).To(Equal("double"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(operator.Linear))

			input := zset.New()
			input.Insert(Record{ID: "a", Value: 5}, 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))

			var found Record
			result.Iter(func(elem zset.Element, weight zset.Weight) bool {
				found = elem.(Record)
				return false
			})
			Expect(found.Value).To(Equal(10))
		})

		It("skips nil results", func() {
			projection := expr.Func(func(e zset.Element) (any, error) {
				if e.(Record).Value > 5 {
					return e, nil
				}
				return nil, nil
			})
			op := operator.NewProject("filter", projection)

			input := zset.New()
			input.Insert(Record{ID: "a", Value: 3}, 1)
			input.Insert(Record{ID: "b", Value: 7}, 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))
			Expect(result.Lookup(Record{ID: "b"})).To(Equal(zset.Weight(1)))
		})
	})

	Describe("CartesianProduct", func() {
		It("computes all pairs", func() {
			op := operator.NewCartesianProduct("x")
			Expect(op.Name()).To(Equal("x"))
			Expect(op.Arity()).To(Equal(2))
			Expect(op.Linearity()).To(Equal(operator.Bilinear))

			left := zset.New()
			left.Insert(StringElem("a"), 1)
			left.Insert(StringElem("b"), 2)

			right := zset.New()
			right.Insert(StringElem("x"), 1)
			right.Insert(StringElem("y"), 3)

			result, err := op.Apply(left, right)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(4))
		})

		It("multiplies weights", func() {
			op := operator.NewCartesianProduct("x")

			left := zsetOf(StringElem("a"), 2)
			right := zsetOf(StringElem("x"), 3)

			result, err := op.Apply(left, right)
			Expect(err).NotTo(HaveOccurred())

			var weight zset.Weight
			result.Iter(func(elem zset.Element, w zset.Weight) bool {
				weight = w
				return false
			})
			Expect(weight).To(Equal(zset.Weight(6)))
		})

		It("creates Pair elements with correct keys", func() {
			op := operator.NewCartesianProduct("x")

			left := zsetOf(StringElem("a"), 1)
			right := zsetOf(StringElem("b"), 1)

			result, err := op.Apply(left, right)
			Expect(err).NotTo(HaveOccurred())

			result.Iter(func(elem zset.Element, weight zset.Weight) bool {
				pair := elem.(*operator.Pair)
				Expect(pair.Left().(StringElem)).To(Equal(StringElem("a")))
				Expect(pair.Right().(StringElem)).To(Equal(StringElem("b")))
				Expect(pair.Key()).To(Equal("(a,b)"))
				return false
			})
		})
	})

	Describe("Distinct", func() {
		It("collapses positive weights to 1", func() {
			op := operator.NewDistinct("H")
			Expect(op.Name()).To(Equal("H"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(operator.NonLinear))

			input := zset.New()
			input.Insert(Record{ID: "a", Value: 1}, 5)
			input.Insert(Record{ID: "b", Value: 2}, 1)
			input.Insert(Record{ID: "c", Value: 3}, -2) // Negative, should be excluded.

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(2))
			Expect(result.Lookup(Record{ID: "a"})).To(Equal(zset.Weight(1)))
			Expect(result.Lookup(Record{ID: "b"})).To(Equal(zset.Weight(1)))
			Expect(result.Lookup(Record{ID: "c"})).To(Equal(zset.Weight(0)))
		})
	})

	Describe("Group", func() {
		It("aggregates by key", func() {
			// Group by first letter of ID, sum values.
			keyExpr := expr.Func(func(e zset.Element) (any, error) {
				return string(e.(Record).ID[0]), nil
			})
			zeroExpr := expr.Func(func(e zset.Element) (any, error) {
				return 0, nil
			})
			foldExpr := expr.Func(func(e zset.Element) (any, error) {
				fi := e.(operator.FoldInput)
				acc := fi.Acc().(int)
				rec := fi.Elem().(Record)
				return acc + rec.Value*int(fi.Weight()), nil
			})
			outputExpr := expr.Func(func(e zset.Element) (any, error) {
				go_ := e.(operator.GroupOutput)
				key := go_.GroupKey().(string)
				sum := go_.Acc().(int)
				return Record{ID: key, Value: sum}, nil
			})

			op := operator.NewGroup("sum_by_first_letter", keyExpr, zeroExpr, foldExpr, outputExpr)
			Expect(op.Name()).To(Equal("sum_by_first_letter"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(operator.NonLinear))

			input := zset.New()
			input.Insert(Record{ID: "a1", Value: 10}, 1)
			input.Insert(Record{ID: "a2", Value: 20}, 1)
			input.Insert(Record{ID: "b1", Value: 5}, 2)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(2))

			// "a" group: 10 + 20 = 30.
			Expect(result.Lookup(Record{ID: "a"})).To(Equal(zset.Weight(1)))
			// "b" group: 5 * 2 = 10.
			Expect(result.Lookup(Record{ID: "b"})).To(Equal(zset.Weight(1)))

			// Verify actual values.
			result.Iter(func(elem zset.Element, weight zset.Weight) bool {
				r := elem.(Record)
				if r.ID == "a" {
					Expect(r.Value).To(Equal(30))
				} else if r.ID == "b" {
					Expect(r.Value).To(Equal(10))
				}
				return true
			})
		})
	})

	Describe("Unwind", func() {
		It("flattens arrays", func() {
			pathExpr := expr.Func(func(e zset.Element) (any, error) {
				return e.(ArrayRecord).Values, nil
			})
			outputExpr := expr.Func(func(e zset.Element) (any, error) {
				ui := e.(operator.UnwindInput)
				orig := ui.Elem().(ArrayRecord)
				// Use index to create unique IDs.
				return Record{ID: fmt.Sprintf("%s_%d", orig.ID, ui.Index()), Value: ui.Element().(int)}, nil
			})

			op := operator.NewUnwind("unwind_values", pathExpr, outputExpr)
			Expect(op.Name()).To(Equal("unwind_values"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(operator.Linear))

			input := zset.New()
			input.Insert(ArrayRecord{ID: "a", Values: []any{1, 2, 3}}, 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(3))
		})

		It("preserves weights", func() {
			pathExpr := expr.Func(func(e zset.Element) (any, error) {
				return e.(ArrayRecord).Values, nil
			})
			outputExpr := expr.Func(func(e zset.Element) (any, error) {
				ui := e.(operator.UnwindInput)
				orig := ui.Elem().(ArrayRecord)
				return Record{ID: orig.ID, Value: ui.Element().(int)}, nil
			})

			op := operator.NewUnwind("unwind", pathExpr, outputExpr)

			input := zset.New()
			input.Insert(ArrayRecord{ID: "a", Values: []any{1}}, 5)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())

			var weight zset.Weight
			result.Iter(func(elem zset.Element, w zset.Weight) bool {
				weight = w
				return false
			})
			Expect(weight).To(Equal(zset.Weight(5)))
		})

		It("handles nil arrays", func() {
			pathExpr := expr.Func(func(e zset.Element) (any, error) {
				return nil, nil
			})
			outputExpr := expr.Func(func(e zset.Element) (any, error) {
				return nil, nil
			})

			op := operator.NewUnwind("unwind", pathExpr, outputExpr)

			input := zset.New()
			input.Insert(ArrayRecord{ID: "a", Values: nil}, 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
		})
	})
})

// Ensure Pair's Key method is accessible from ArrayRecord.
type ArrayRecord struct {
	ID     string
	Values []any
}

func (r ArrayRecord) Key() string { return r.ID }
