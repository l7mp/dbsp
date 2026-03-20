package sql

import (
	"github.com/l7mp/dbsp/engine/datamodel/relation"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/zset"
	"github.com/l7mp/dbsp/engine/internal/logger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SQL end-to-end", func() {
	It("compiles and executes a WHERE clause", func() {
		db := relation.NewDatabase("db")
		table := relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a", QualifiedName: "t.a"}}})
		db.RegisterTable("t", table)
		compiler := NewCompiler(db)
		query, err := compiler.CompileString("select a from t where a = 1")
		Expect(err).NotTo(HaveOccurred())
		exec, err := executor.New(query.Circuit, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())

		row1 := &relation.Row{Table: table, Data: []any{int64(1)}}
		row2 := &relation.Row{Table: table, Data: []any{int64(2)}}
		input := zset.New().WithElems(zset.Elem{Document: row1, Weight: 1}, zset.Elem{Document: row2, Weight: 1})

		outputs, err := exec.Execute(map[string]zset.ZSet{"input_t": input})
		Expect(err).NotTo(HaveOccurred())
		Expect(outputs["output"].Size()).To(Equal(1))
	})

	It("compiles and executes a join", func() {
		db := relation.NewDatabase("db")
		tableA := relation.NewTable("a", &relation.Schema{Columns: []relation.Column{{Name: "id", QualifiedName: "a.id"}}})
		tableB := relation.NewTable("b", &relation.Schema{Columns: []relation.Column{{Name: "id", QualifiedName: "b.id"}}})
		db.RegisterTable("a", tableA)
		db.RegisterTable("b", tableB)
		compiler := NewCompiler(db)
		query, err := compiler.CompileString("select a.id, b.id from a join b on a.id = b.id")
		Expect(err).NotTo(HaveOccurred())
		exec, err := executor.New(query.Circuit, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())

		rowA := &relation.Row{Table: tableA, Data: []any{int64(1)}}
		rowB := &relation.Row{Table: tableB, Data: []any{int64(1)}}
		inputA := zset.New().WithElems(zset.Elem{Document: rowA, Weight: 1})
		inputB := zset.New().WithElems(zset.Elem{Document: rowB, Weight: 1})

		outputs, err := exec.Execute(map[string]zset.ZSet{"input_a": inputA, "input_b": inputB})
		Expect(err).NotTo(HaveOccurred())
		Expect(outputs["output"].Size()).To(Equal(1))
	})
})
