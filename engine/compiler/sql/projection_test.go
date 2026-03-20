package sql

import (
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/relation"
	"github.com/l7mp/dbsp/engine/expression"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/xwb1989/sqlparser"
)

var _ = Describe("Projection helpers", func() {
	It("projects fields from the input document into a new document", func() {
		schema := relation.NewSchema("a", "b").WithQualifiedNames("t").WithPrimaryKey(0)
		table := relation.NewTable("t", schema)
		row := &relation.Row{Table: table, Data: []any{int64(1), int64(2)}}

		// Build a minimal SELECT a, b expression list.
		exprs := sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{
				Name:      sqlparser.NewColIdent("a"),
				Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t")},
			}},
			&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{
				Name:      sqlparser.NewColIdent("b"),
				Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t")},
			}},
		}

		projExpr, err := compileProjection(exprs, nil)
		Expect(err).NotTo(HaveOccurred())

		// The projection must read from the input row and write to a new doc.
		result, err := projExpr.Evaluate(expression.NewContext(row))
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(BeNil())

		newDoc, ok := result.(datamodel.Document)
		Expect(ok).To(BeTrue())

		aVal, err := newDoc.GetField("a")
		Expect(err).NotTo(HaveOccurred())
		Expect(aVal).To(Equal(int64(1)))

		bVal, err := newDoc.GetField("b")
		Expect(err).NotTo(HaveOccurred())
		Expect(bVal).To(Equal(int64(2)))
	})
})
