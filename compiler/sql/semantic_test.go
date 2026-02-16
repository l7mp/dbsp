package sql

import (
	"github.com/l7mp/dbsp/datamodel/relation"
	"github.com/l7mp/dbsp/expression"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SQL semantics", func() {
	It("handles NULL semantics for AND/NOT", func() {
		row := &relation.Row{
			Table: relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a"}}}),
			Data:  []any{nil},
		}

		db := relation.NewDatabase("db")
		table := relation.NewTable("t", relation.NewSchema("a").WithQualifiedNames("t"))
		db.RegisterTable("t", table)
		norm, err := Normalize("select * from t where (a = 1) and true", db)
		Expect(err).NotTo(HaveOccurred())
		predicate, err := CompilePredicate(norm.Stmt.Where.Expr, norm.BindVars)
		Expect(err).NotTo(HaveOccurred())
		val, err := predicate.Evaluate(expression.NewContext(row))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())
	})

	It("handles NULL semantics for OR", func() {
		row := &relation.Row{
			Table: relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a"}}}),
			Data:  []any{nil},
		}
		db := relation.NewDatabase("db")
		table := relation.NewTable("t", relation.NewSchema("a").WithQualifiedNames("t"))
		db.RegisterTable("t", table)
		norm, err := Normalize("select * from t where (a = 1) or false", db)
		Expect(err).NotTo(HaveOccurred())
		predicate, err := CompilePredicate(norm.Stmt.Where.Expr, norm.BindVars)
		Expect(err).NotTo(HaveOccurred())
		val, err := predicate.Evaluate(expression.NewContext(row))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())
	})

	It("builds projection expression", func() {
		db := relation.NewDatabase("db")
		table := relation.NewTable("t", relation.NewSchema("a").WithQualifiedNames("t"))
		db.RegisterTable("t", table)
		_, err := Normalize("select * from t", db)
		Expect(err).NotTo(HaveOccurred())
		_, err = Normalize("select a from t", db)
		Expect(err).NotTo(HaveOccurred())
	})
})
