package sql

import (
	"encoding/json"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/relation"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/expression"
	"github.com/l7mp/dbsp/engine/internal/logger"
	"github.com/l7mp/dbsp/engine/zset"
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

	It("round-trips compiled SQL projection circuits", func() {
		db := relation.NewDatabase("db")
		table := relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a", QualifiedName: "t.a"}, {Name: "b", QualifiedName: "t.b"}}})
		db.RegisterTable("t", table)

		compiler := New(db)
		query, err := compiler.CompileString("select a, b from t")
		Expect(err).NotTo(HaveOccurred())

		payload, err := query.Circuit.MarshalJSON()
		Expect(err).NotTo(HaveOccurred())

		var cloned circuit.Circuit
		Expect(json.Unmarshal(payload, &cloned)).To(Succeed())

		execOrig, err := executor.New(query.Circuit, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())
		execClone, err := executor.New(&cloned, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())

		row := &relation.Row{Table: table, Data: []any{int64(1), int64(2)}}
		input := zset.New().WithElems(zset.Elem{Document: row, Weight: 1})

		outsOrig, err := execOrig.Execute(map[string]zset.ZSet{"input_t": input})
		Expect(err).NotTo(HaveOccurred())
		outsClone, err := execClone.Execute(map[string]zset.ZSet{"input_t": input})
		Expect(err).NotTo(HaveOccurred())
		outID := query.OutputMap["output"]
		Expect(outsClone[outID].Equal(outsOrig[outID])).To(BeTrue())
	})
})
