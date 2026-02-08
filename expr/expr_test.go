package expr_test

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/zset"
)

func TestExpr(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Expr Suite")
}

type StringElem string

func (s StringElem) Key() any { return string(s) }

var _ = Describe("Expression", func() {
	Describe("Func", func() {
		It("wraps a function as Expression", func() {
			fn := expr.Func(func(elem zset.Element) (any, error) {
				return string(elem.(StringElem)) + "!", nil
			})

			result, err := fn.Evaluate(StringElem("hello"))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal("hello!"))
		})

		It("propagates errors", func() {
			expectedErr := errors.New("test error")
			fn := expr.Func(func(elem zset.Element) (any, error) {
				return nil, expectedErr
			})

			_, err := fn.Evaluate(StringElem("x"))
			Expect(err).To(Equal(expectedErr))
		})

		It("handles nil element", func() {
			fn := expr.Func(func(elem zset.Element) (any, error) {
				if elem == nil {
					return "nil", nil
				}
				return "not nil", nil
			})

			result, err := fn.Evaluate(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal("nil"))
		})
	})
})
