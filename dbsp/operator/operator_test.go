package operator

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/expression"
	exprdbsp "github.com/l7mp/dbsp/expression/dbsp"
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

	Describe("LinearCombination", func() {
		var (
			x, y, z zset.ZSet
			rx, ry  testutils.Record
		)
		BeforeEach(func() {
			rx = testutils.Record{ID: "x", Value: 1}
			ry = testutils.Record{ID: "y", Value: 2}
			x = zset.New()
			x.Insert(rx, 3)
			y = zset.New()
			y.Insert(ry, 4)
			z = zset.New()
			z.Insert(rx, 1)
		})

		It("has linear linearity and correct arity", func() {
			op := NewLinearCombination([]int{1, -1})
			Expect(op.Linearity()).To(Equal(Linear))
			Expect(op.Arity()).To(Equal(2))
		})

		It("computes X + Y with coefficients [+1, +1]", func() {
			op := NewLinearCombination([]int{1, 1})
			result, err := op.Apply(x, y)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(rx.Hash())).To(Equal(zset.Weight(3)))
			Expect(result.Lookup(ry.Hash())).To(Equal(zset.Weight(4)))
		})

		It("computes X - Y with coefficients [+1, -1]", func() {
			op := NewLinearCombination([]int{1, -1})
			result, err := op.Apply(x, x)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(rx.Hash())).To(Equal(zset.Weight(0)))
		})

		It("computes X + Y - Z with coefficients [+1, +1, -1]", func() {
			op := NewLinearCombination([]int{1, 1, -1})
			Expect(op.Arity()).To(Equal(3))
			result, err := op.Apply(x, y, z)
			Expect(err).NotTo(HaveOccurred())
			// rx: weight 3 + 0 - 1 = 2
			Expect(result.Lookup(rx.Hash())).To(Equal(zset.Weight(2)))
			// ry: weight 0 + 4 - 0 = 4
			Expect(result.Lookup(ry.Hash())).To(Equal(zset.Weight(4)))
		})

		It("scales with coefficient 2", func() {
			op := NewLinearCombination([]int{2})
			result, err := op.Apply(x)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(rx.Hash())).To(Equal(zset.Weight(6)))
		})

		It("drops inputs with coefficient 0", func() {
			op := NewLinearCombination([]int{0, 1})
			result, err := op.Apply(x, y)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Lookup(rx.Hash())).To(Equal(zset.Weight(0)))
			Expect(result.Lookup(ry.Hash())).To(Equal(zset.Weight(4)))
		})
	})

	Describe("Select", func() {
		It("filters elements by predicate", func() {
			predicate := expression.Func(func(ctx *expression.EvalContext) (any, error) {
				e := ctx.Document().(testutils.Record)
				return e.Value > 5, nil
			})
			op := NewSelect(predicate)
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
			op := NewSelect(predicate)

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
			op := NewProject(projection)
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
			op := NewProject(projection)

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
			op := NewCartesianProduct()
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
			op := NewCartesianProduct()

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
			op := NewCartesianProduct()

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

	Describe("Aggregate", func() {
		It("supports distinct-pi semantics via lexmin reducer", func() {
			op := NewDistinctPi()

			r1 := testutils.Record{ID: "a", Value: 1}
			r2 := testutils.Record{ID: "a", Value: 2}
			Expect(r1.Hash() < r2.Hash()).To(BeTrue())

			delta := zset.New()
			delta.Insert(r1, 1)
			delta.Insert(r2, 1)

			result, err := op.Apply(delta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))

			var row datamodel.Document
			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				Expect(weight).To(Equal(zset.Weight(1)))
				row = elem
				return false
			})

			v, _ := row.GetField("value")
			selected, ok := v.(datamodel.Document)
			Expect(ok).To(BeTrue())
			Expect(selected.Hash()).To(Equal(r1.Hash()))
		})

		It("supports gather-style list aggregation", func() {
			op := NewAggregate(nil, exprdbsp.NewGet("value"), exprdbsp.NewSubject(), "items")

			r1 := testutils.Record{ID: "ns-a", Value: 1}
			r2 := testutils.Record{ID: "ns-a", Value: 2}
			delta := zset.New()
			delta.Insert(r1, 1)
			delta.Insert(r2, 1)

			result, err := op.Apply(delta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))

			var row datamodel.Document
			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				Expect(weight).To(Equal(zset.Weight(1)))
				row = elem
				return false
			})

			k, _ := row.GetField("key")
			items, _ := row.GetField("items")
			Expect(k).To(Equal("ns-a"))
			Expect(items.([]any)).To(ConsistOf(1, 2))
		})

		It("supports set-expr output on representative document", func() {
			op := NewAggregateWithSet(
				nil,
				expression.Func(func(ctx *expression.EvalContext) (any, error) {
					return ctx.Subject(), nil
				}),
				exprdbsp.NewLen(expression.Func(func(ctx *expression.EvalContext) (any, error) { return ctx.Subject(), nil })),
				exprdbsp.NewSet(exprdbsp.NewString("Value"), expression.Func(func(ctx *expression.EvalContext) (any, error) { return ctx.Subject(), nil })),
			)

			r1 := &testutils.MutableRecord{FieldMap: map[string]any{"id": "ns-a", "value": int64(2)}}

			delta := zset.New()
			delta.Insert(r1, 1)

			result, err := op.Apply(delta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))

			var out datamodel.Document
			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				Expect(weight).To(Equal(zset.Weight(1)))
				out = elem
				return false
			})

			total, err := out.GetField("Value")
			Expect(err).NotTo(HaveOccurred())
			Expect(total).To(Equal(int64(1)))
		})

		It("supports count via @len($. )", func() {
			op := NewAggregate(nil, nil, exprdbsp.NewLen(expression.Func(func(ctx *expression.EvalContext) (any, error) {
				return ctx.Subject(), nil
			})), "value")

			r1 := testutils.Record{ID: "ns-a", Value: 1}
			r2 := testutils.Record{ID: "ns-a", Value: 2}
			delta := zset.New()
			delta.Insert(r1, 1)
			delta.Insert(r2, 1)

			result, err := op.Apply(delta)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Size()).To(Equal(1))

			var out datamodel.Document
			result.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
				Expect(weight).To(Equal(zset.Weight(1)))
				out = elem
				return false
			})
			k, _ := out.GetField("key")
			v, _ := out.GetField("value")
			Expect(k).To(Equal("ns-a"))
			Expect(v).To(Equal(int64(2)))
		})

	})

	Describe("Distinct", func() {
		It("collapses positive weights to 1", func() {
			op := NewDistinct()
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
			op := NewUnwind("values")
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
			op := NewUnwind("values")

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
			op := NewUnwind("tags")

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
			op := NewUnwind("values").WithIndexField("idx")

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
			op := NewUnwind("values")

			input := zset.New()
			input.Insert(testutils.NewMutableRecord(map[string]any{
				"id": "no_values",
			}), 1)

			result, err := op.Apply(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
		})

		It("handles nil array values", func() {
			op := NewUnwind("values")

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
