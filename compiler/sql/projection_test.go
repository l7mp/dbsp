package sql

import (
	"github.com/l7mp/dbsp/datamodel/relation"
	"github.com/l7mp/dbsp/expression"
	"github.com/l7mp/dbsp/expression/dbsp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Projection helpers", func() {
	It("compiles projection into @set chain", func() {
		row := &relation.Row{
			Table: relation.NewTable("t", relation.NewSchema("a", "b").WithQualifiedNames("t")),
			Data:  []any{int64(1), int64(2)},
		}
		expr, err := compileProjectionSet(map[string]dbsp.Expression{
			"a": dbsp.NewGet("t.a"),
			"b": dbsp.NewGet("t.b"),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = expr.Evaluate(expression.NewContext(row))
		Expect(err).NotTo(HaveOccurred())
		Expect(row.GetField("t.a")).To(Equal(int64(1)))
		Expect(row.GetField("t.b")).To(Equal(int64(2)))
	})
})
