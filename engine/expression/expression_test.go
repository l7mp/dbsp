package expression

import (
	"errors"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/internal/testutils"
)

// testConst is a test-only constant expression.
type testConst struct{ value any }

func newTestConst(value any) *testConst                   { return &testConst{value: value} }
func (c *testConst) Evaluate(_ *EvalContext) (any, error) { return c.value, nil }
func (c *testConst) String() string                       { return fmt.Sprintf("Const(%v)", c.value) }

func TestExpr(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Expr Suite")
}

var _ = Describe("Expression", func() {
	Describe("Func", func() {
		It("wraps a function as Expression", func() {
			fn := Func(func(ctx *EvalContext) (any, error) {
				return string(ctx.Document().(testutils.StringElem)) + "!", nil
			})

			result, err := fn.Evaluate(NewContext(testutils.StringElem("hello")))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal("hello!"))
		})

		It("implements fmt.Stringer", func() {
			fn := Func(func(ctx *EvalContext) (any, error) {
				return nil, nil
			})
			Expect(fn.String()).To(Equal("Func"))
		})

		It("propagates errors", func() {
			expectedErr := errors.New("test error")
			fn := Func(func(ctx *EvalContext) (any, error) {
				return nil, expectedErr
			})

			_, err := fn.Evaluate(NewContext(testutils.StringElem("x")))
			Expect(err).To(Equal(expectedErr))
		})

		It("handles nil element", func() {
			fn := Func(func(ctx *EvalContext) (any, error) {
				if ctx.Document() == nil {
					return "nil", nil
				}
				return "not nil", nil
			})

			result, err := fn.Evaluate(NewContext(nil))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal("nil"))
		})
	})

	Describe("Const", func() {
		It("returns constant value", func() {
			c := newTestConst(42)
			result, err := c.Evaluate(NewContext(testutils.StringElem("ignored")))
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(42))
		})

		It("implements fmt.Stringer", func() {
			c := newTestConst("hello")
			Expect(c.String()).To(Equal("Const(hello)"))
		})
	})
})
