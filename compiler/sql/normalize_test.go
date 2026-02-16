package sql

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/xwb1989/sqlparser"

	"github.com/l7mp/dbsp/datamodel/relation"
)

var _ = Describe("Normalize", func() {
	It("rewrites alias-qualified columns", func() {
		db := relation.NewDatabase("db")
		schema := relation.NewSchema("a").WithQualifiedNames("t")
		table := relation.NewTable("t", schema)
		db.RegisterTable("t", table)

		norm, err := Normalize("select a from t as x where x.a = 1", db)
		Expect(err).NotTo(HaveOccurred())
		Expect(norm.Stmt.Where).NotTo(BeNil())

		cmp, ok := norm.Stmt.Where.Expr.(*sqlparser.ComparisonExpr)
		Expect(ok).To(BeTrue())
		col, ok := cmp.Left.(*sqlparser.ColName)
		Expect(ok).To(BeTrue())
		Expect(col.Qualifier.Name.String()).To(Equal("t"))
	})

	It("rewrites join aliases in ON", func() {
		db := relation.NewDatabase("db")
		tableA := relation.NewTable("a", relation.NewSchema("id").WithQualifiedNames("a"))
		tableB := relation.NewTable("b", relation.NewSchema("id").WithQualifiedNames("b"))
		db.RegisterTable("a", tableA)
		db.RegisterTable("b", tableB)

		norm, err := Normalize("select * from a as x join b as y on x.id = y.id", db)
		Expect(err).NotTo(HaveOccurred())
		join, ok := norm.Stmt.From[0].(*sqlparser.JoinTableExpr)
		Expect(ok).To(BeTrue())

		cmp, ok := join.Condition.On.(*sqlparser.ComparisonExpr)
		Expect(ok).To(BeTrue())
		left, ok := cmp.Left.(*sqlparser.ColName)
		Expect(ok).To(BeTrue())
		right, ok := cmp.Right.(*sqlparser.ColName)
		Expect(ok).To(BeTrue())
		Expect(left.Qualifier.Name.String()).To(Equal("a"))
		Expect(right.Qualifier.Name.String()).To(Equal("b"))
	})

	It("expands star projection with alias", func() {
		db := relation.NewDatabase("db")
		schema := relation.NewSchema("a", "b").WithQualifiedNames("t")
		table := relation.NewTable("t", schema)
		db.RegisterTable("t", table)

		norm, err := Normalize("select x.* from t as x", db)
		Expect(err).NotTo(HaveOccurred())
		Expect(norm.Stmt.SelectExprs).To(HaveLen(2))
		col1 := norm.Stmt.SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
		col2 := norm.Stmt.SelectExprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
		Expect(col1.Qualifier.Name.String()).To(Equal("t"))
		Expect(col2.Qualifier.Name.String()).To(Equal("t"))
	})
})
