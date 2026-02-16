package operator

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/expression"
	"github.com/l7mp/dbsp/internal/testutils"
)

func TestOperator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Operator Suite")
}

var _ = Describe("Operators", func() {
	Describe("Linearity", func() {
		It("has correct string representation", func() {
			Expect(Linear.String()).To(Equal("Linear"))
			Expect(Bilinear.String()).To(Equal("Bilinear"))
			Expect(NonLinear.String()).To(Equal("NonLinear"))
		})
	})

	Describe("Negate", func() {
		It("negates all weights", func() {
			op := NewNegate()
			// op := NewNegate(WithLogger(logger.NewZapLogger(logger.TraceLevel)))
			Expect(op.Name()).To(Equal("negate"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(Linear))

			input := zset.New()
			record1 := testutils.Record{ID: "a", Value: 1}
			record2 := testutils.Record{ID: "b", Value: 2}
			input.Insert(record1, 3)
			input.Insert(record2, -2)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(2))
			Expect(result.Lookup(record1.Hash())).To(Equal(zset.Weight(-3)))
			Expect(result.Lookup(record2.Hash())).To(Equal(zset.Weight(2)))
		})
	})

	Describe("Plus", func() {
		It("adds two Z-sets", func() {
			op := NewPlus()
			Expect(op.Name()).To(Equal("plus"))
			Expect(op.Arity()).To(Equal(2))
			Expect(op.Linearity()).To(Equal(Linear))

			a := zset.New()
			a.Insert(testutils.Record{ID: "x", Value: 1}, 1)

			b := zset.New()
			b.Insert(testutils.Record{ID: "x", Value: 1}, 2)
			b.Insert(testutils.Record{ID: "y", Value: 2}, 3)

			result, err := op.Apply(a, b)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(testutils.Record{ID: "x", Value: 1}.Hash())).To(Equal(zset.Weight(3)))
			Expect(result.Lookup(testutils.Record{ID: "y", Value: 2}.Hash())).To(Equal(zset.Weight(3)))
		})
	})

	Describe("Select", func() {
		It("filters elements by predicate", func() {
			predicate := expression.Func(func(ctx *expression.EvalContext) (any, error) {
				e := ctx.Document().(testutils.Record)
				return e.Value > 5, nil
			})
			op := NewSelect("gt5", predicate)
			Expect(op.Name()).To(Equal("gt5"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(Linear))

			recordA := testutils.Record{ID: "a", Value: 3}
			recordB := testutils.Record{ID: "b", Value: 7}
			recordC := testutils.Record{ID: "c", Value: 10}
			input := zset.New()
			input.Insert(recordA, 1)
			input.Insert(recordB, 2)
			input.Insert(recordC, 3)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(2))
			Expect(result.Lookup(recordB.Hash())).To(Equal(zset.Weight(2)))
			Expect(result.Lookup(recordC.Hash())).To(Equal(zset.Weight(3)))
		})

		It("preserves weights", func() {
			predicate := expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return true, nil
			})
			op := NewSelect("all", predicate)

			recordA := testutils.Record{ID: "a", Value: 1}
			input := zset.New()
			input.Insert(recordA, 5)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(recordA.Hash())).To(Equal(zset.Weight(5)))
		})
	})

	Describe("Project", func() {
		It("transforms elements", func() {
			projection := expression.Func(func(ctx *expression.EvalContext) (any, error) {
				r := ctx.Document().(testutils.Record)
				return testutils.Record{ID: r.ID, Value: r.Value * 2}, nil
			})
			op := NewProject("double", projection)
			Expect(op.Name()).To(Equal("double"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(Linear))

			input := zset.New()
			input.Insert(testutils.Record{ID: "a", Value: 5}, 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))

			var found testutils.Record
			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				found = elem.(testutils.Record)
				return false
			})
			Expect(found.Value).To(Equal(10))
		})

		It("skips nil results", func() {
			projection := expression.Func(func(ctx *expression.EvalContext) (any, error) {
				e := ctx.Document().(testutils.Record)
				if e.Value > 5 {
					return e, nil
				}
				return nil, nil
			})
			op := NewProject("filter", projection)

			recordA := testutils.Record{ID: "a", Value: 3}
			recordB := testutils.Record{ID: "b", Value: 7}
			input := zset.New()
			input.Insert(recordA, 1)
			input.Insert(recordB, 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))
			Expect(result.Lookup(recordB.Hash())).To(Equal(zset.Weight(1)))
		})
	})

	Describe("CartesianProduct", func() {
		It("computes all pairs", func() {
			op := NewCartesianProduct("x")
			Expect(op.Name()).To(Equal("x"))
			Expect(op.Arity()).To(Equal(2))
			Expect(op.Linearity()).To(Equal(Bilinear))

			left := zset.New()
			left.Insert(testutils.StringElem("a"), 1)
			left.Insert(testutils.StringElem("b"), 2)

			right := zset.New()
			right.Insert(testutils.StringElem("x"), 1)
			right.Insert(testutils.StringElem("y"), 3)

			result, err := op.Apply(left, right)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(4))
		})

		It("multiplies weights", func() {
			op := NewCartesianProduct("x")

			left := zset.New().WithElems(zset.Elem{Document: testutils.StringElem("a"), Weight: 2})
			right := zset.New().WithElems(zset.Elem{Document: testutils.StringElem("x"), Weight: 3})

			result, err := op.Apply(left, right)
			Expect(err).NotTo(HaveOccurred())

			var weight zset.Weight
			result.Iter(func(elem datamodel.Document, w zset.Weight) bool {
				weight = w
				return false
			})
			Expect(weight).To(Equal(zset.Weight(6)))
		})

		It("creates concatenated elements with correct keys", func() {
			op := NewCartesianProduct("x")

			left := zset.New().WithElems(zset.Elem{Document: testutils.StringElem("a"), Weight: 1})
			right := zset.New().WithElems(zset.Elem{Document: testutils.StringElem("b"), Weight: 1})

			result, err := op.Apply(left, right)
			Expect(err).NotTo(HaveOccurred())

			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				// testutils.StringElem.Concat returns "a,b"
				Expect(elem.(testutils.StringElem)).To(Equal(testutils.StringElem("a,b")))
				Expect(elem.Hash()).To(Equal("a,b"))
				return false
			})
		})
	})

	Describe("Distinct", func() {
		It("collapses positive weights to 1", func() {
			op := NewDistinct("H")
			Expect(op.Name()).To(Equal("H"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(NonLinear))

			recordA := testutils.Record{ID: "a", Value: 1}
			recordB := testutils.Record{ID: "b", Value: 2}
			recordC := testutils.Record{ID: "c", Value: 3}
			input := zset.New()
			input.Insert(recordA, 5)
			input.Insert(recordB, 1)
			input.Insert(recordC, -2) // Negative, should be excluded.

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(2))
			Expect(result.Lookup(recordA.Hash())).To(Equal(zset.Weight(1)))
			Expect(result.Lookup(recordB.Hash())).To(Equal(zset.Weight(1)))
			Expect(result.Lookup(recordC.Hash())).To(Equal(zset.Weight(0)))
		})
	})

	Describe("Unwind", func() {
		It("flattens arrays", func() {
			op := NewUnwind("unwind_values", "values")
			Expect(op.Name()).To(Equal("unwind_values"))
			Expect(op.Arity()).To(Equal(1))
			Expect(op.Linearity()).To(Equal(Linear))

			input := zset.New()
			input.Insert(testutils.NewMutableRecord(map[string]any{
				"id":     "a",
				"values": []any{1, 2, 3},
			}), 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(3))

			// Verify each unwound element.
			values := []any{}
			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				v, _ := elem.GetField("values")
				values = append(values, v)
				Expect(weight).To(Equal(zset.Weight(1)))
				return true
			})
			Expect(values).To(ConsistOf(1, 2, 3))
		})

		It("preserves weights", func() {
			op := NewUnwind("unwind", "values")

			input := zset.New()
			input.Insert(testutils.NewMutableRecord(map[string]any{
				"id":     "a",
				"values": []any{1},
			}), 5)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())

			var weight zset.Weight
			result.Iter(func(elem datamodel.Document, w zset.Weight) bool {
				weight = w
				return false
			})
			Expect(weight).To(Equal(zset.Weight(5)))
		})

		It("preserves other fields", func() {
			op := NewUnwind("unwind", "tags")

			input := zset.New()
			input.Insert(testutils.NewMutableRecord(map[string]any{
				"id":   "doc1",
				"name": "Test",
				"tags": []any{"a", "b"},
			}), 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(2))

			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				id, _ := elem.GetField("id")
				name, _ := elem.GetField("name")
				Expect(id).To(Equal("doc1"))
				Expect(name).To(Equal("Test"))
				return true
			})
		})

		It("adds index field when configured", func() {
			op := NewUnwind("unwind", "values").WithIndexField("idx")

			input := zset.New()
			input.Insert(testutils.NewMutableRecord(map[string]any{
				"id":     "a",
				"values": []any{"x", "y", "z"},
			}), 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(3))

			indices := []int64{}
			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				idx, _ := elem.GetField("idx")
				indices = append(indices, idx.(int64))
				return true
			})
			Expect(indices).To(ConsistOf(int64(0), int64(1), int64(2)))
		})

		It("skips documents with missing array field", func() {
			op := NewUnwind("unwind", "values")

			input := zset.New()
			input.Insert(testutils.NewMutableRecord(map[string]any{
				"id": "no_values",
			}), 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
		})

		It("handles nil array values", func() {
			op := NewUnwind("unwind", "values")

			input := zset.New()
			input.Insert(testutils.NewMutableRecord(map[string]any{
				"id":     "a",
				"values": nil,
			}), 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
		})
	})
})
