package sql

import (
	"github.com/l7mp/dbsp/datamodel/relation"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/xwb1989/sqlparser"
)

var _ = Describe("CompileExpression", func() {
	It("compiles basic WHERE expressions", func() {
		db := relation.NewDatabase("db")
		table := relation.NewTable("t", relation.NewSchema("a", "b").WithQualifiedNames("t"))
		db.RegisterTable("t", table)
		norm, err := Normalize("select * from t where a = 1 and b is null", db)
		Expect(err).NotTo(HaveOccurred())
		Expect(norm.Stmt.Where).NotTo(BeNil())

		expr, err := CompilePredicate(norm.Stmt.Where.Expr, norm.BindVars)
		Expect(err).NotTo(HaveOccurred())
		Expect(expr).NotTo(BeNil())
	})

	It("compiles expressions with join aliases", func() {
		db := relation.NewDatabase("db")
		tableA := relation.NewTable("a", relation.NewSchema("id").WithQualifiedNames("a"))
		tableB := relation.NewTable("b", relation.NewSchema("id").WithQualifiedNames("b"))
		db.RegisterTable("a", tableA)
		db.RegisterTable("b", tableB)
		norm, err := Normalize("select * from a join b on a.id = b.id", db)
		Expect(err).NotTo(HaveOccurred())
		join, ok := norm.Stmt.From[0].(*sqlparser.JoinTableExpr)
		Expect(ok).To(BeTrue())
		_, err = CompilePredicate(join.Condition.On, norm.BindVars)
		Expect(err).NotTo(HaveOccurred())
	})
})
