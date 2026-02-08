package sql

import (
	"bytes"
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSQL(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SQL Suite")
}

// Simple demo showing the pieces working together
var _ = Describe("SQL Basics", func() {
	It("evaluates a simple SQL statement", func() {
		// 1. Define Schema
		schema := &Schema{
			Columns: []Column{
				{Name: "id", Type: TypeInt},
				{Name: "name", Type: TypeString},
				{Name: "age", Type: TypeInt},
			},
			PKIndices: []int{0}, // ID is PK
		}

		// 2. Create Table
		tbl := NewTable("users", schema)
		tbl.Insert([]any{1, "Alice", 30})
		tbl.Insert([]any{2, "Bob", 45})
		tbl.Insert([]any{3, "Charlie", 25})

		// 3. Serialize/Deserialize Test
		var buf bytes.Buffer
		DumpTable(tbl, &buf)
		fmt.Println("DB Dump JSONL:\n", buf.String())

		// 4. Expression Test (Filter)
		// "age > 28"
		expr, err := ParseExpression("age > 28")
		Expect(err).NotTo(HaveOccurred())

		fmt.Println("Filtering: WHERE age > 28")
		zset := tbl.ToZSet()
		for _, value := range zset.Entries() {
			res, ok := expr.Evaluate(value.Document)
			Expect(ok).To(BeTrue())
			if res == true {
				fmt.Printf("MATCH: %v\n", value.Document.(*Row).Data)
			}
		}
	})
})
