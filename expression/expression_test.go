package expression

import (
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/internal/testutils"
)

func TestExpr(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Expr Suite")
}

var _ = Describe("Expression", func() {
	Describe("Func", func() {
		It("wraps a function as Expression", func() {
			fn := Func(func(elem datamodel.Document) (any, error) {
				return string(elem.(testutils.StringElem)) + "!", nil
			})

			result, err := fn.Evaluate(testutils.StringElem("hello"))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal("hello!"))
		})

		It("implements fmt.Stringer", func() {
			fn := Func(func(elem datamodel.Document) (any, error) {
				return nil, nil
			})
			Expect(fn.String()).To(Equal("Func"))
		})

		It("propagates errors", func() {
			expectedErr := errors.New("test error")
			fn := Func(func(elem datamodel.Document) (any, error) {
				return nil, expectedErr
			})

			_, err := fn.Evaluate(testutils.StringElem("x"))
			Expect(err).To(Equal(expectedErr))
		})

		It("handles nil element", func() {
			fn := Func(func(elem datamodel.Document) (any, error) {
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

	Describe("Const", func() {
		It("returns constant value", func() {
			c := NewConst(42)
			result, err := c.Evaluate(testutils.StringElem("ignored"))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(42))
		})

		It("implements fmt.Stringer", func() {
			c := NewConst("hello")
			Expect(c.String()).To(Equal("Const(hello)"))
		})
	})
})
