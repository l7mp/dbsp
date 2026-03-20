package relation

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
)

func makeSchema(columns ...Column) *Schema {
	return &Schema{Columns: columns}
}

func makeTable(name string, schema *Schema) *Table {
	return NewTable(name, schema)
}

func makeRow(table *Table, data ...any) *Row {
	return &Row{Table: table, Data: data}
}

func rowAsRow(doc datamodel.Document) *Row {
	row, ok := doc.(*Row)
	Expect(ok).To(BeTrue())
	return row
}

func product(a, b []datamodel.Document) []datamodel.Document {
	result := make([]datamodel.Document, 0, len(a)*len(b))
	for _, left := range a {
		for _, right := range b {
			result = append(result, left.Merge(right))
		}
	}
	return result
}

func joinOn(leftIdx int, rightIdx int, left, right []datamodel.Document) []datamodel.Document {
	result := make([]datamodel.Document, 0)
	for _, leftDoc := range left {
		lrow := rowAsRow(leftDoc)
		lval := lrow.Data[leftIdx]
		for _, rightDoc := range right {
			rrow := rowAsRow(rightDoc)
			rval := rrow.Data[rightIdx]
			if lval == rval {
				result = append(result, leftDoc.Merge(rightDoc))
			}
		}
	}
	return result
}

var _ = Describe("Row", func() {
	Describe("Concat", func() {
		It("concatenates data and prefixes columns", func() {
			leftSchema := makeSchema(
				Column{Name: "id", QualifiedName: "employees.id", Type: TypeInt},
				Column{Name: "name", QualifiedName: "employees.name", Type: TypeString},
			)
			rightSchema := makeSchema(
				Column{Name: "dept", QualifiedName: "comp.dept", Type: TypeString},
				Column{Name: "salary", QualifiedName: "comp.salary", Type: TypeInt},
			)
			leftTable := makeTable("employees", leftSchema)
			rightTable := makeTable("comp", rightSchema)

			left := makeRow(leftTable, 1, "Alice")
			right := makeRow(rightTable, "eng", 100)

			combined := rowAsRow(left.Merge(right))

			Expect(combined.Table).NotTo(BeNil())
			Expect(combined.Table.Name).To(Equal("employees-comp"))
			Expect(combined.Data).To(Equal([]any{1, "Alice", "eng", 100}))

			Expect(combined.Table.Schema.Columns).To(HaveLen(4))
			Expect(combined.Table.Schema.Columns[0].QualifiedName).To(Equal("employees.id"))
			Expect(combined.Table.Schema.Columns[1].QualifiedName).To(Equal("employees.name"))
			Expect(combined.Table.Schema.Columns[2].QualifiedName).To(Equal("comp.dept"))
			Expect(combined.Table.Schema.Columns[3].QualifiedName).To(Equal("comp.salary"))
		})

		It("returns a copy when merging non-rows", func() {
			leftSchema := makeSchema(Column{Name: "id", Type: TypeInt})
			leftTable := makeTable("t1", leftSchema)
			left := makeRow(leftTable, 1)

			result := left.Merge(badDoc{id: "x"})
			Expect(result).To(Equal(left))
			Expect(result).NotTo(BeIdenticalTo(left))
		})
	})

	Describe("GetField/SetField", func() {
		It("gets and sets by column name", func() {
			schema := makeSchema(
				Column{Name: "id", QualifiedName: "people.id", Type: TypeInt},
				Column{Name: "name", QualifiedName: "people.name", Type: TypeString},
			)
			table := makeTable("people", schema)
			row := makeRow(table, 1, "Alice")

			value, err := row.GetField("name")
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal("Alice"))

			Expect(row.SetField("name", "Bob")).To(Succeed())
			value, err = row.GetField("name")
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal("Bob"))
		})

		It("uses aliases for lookups", func() {
			schema := makeSchema(
				Column{Name: "id", QualifiedName: "people.id", Type: TypeInt},
				Column{Name: "full_name", QualifiedName: "people.full_name", Type: TypeString},
			)
			schema.Aliases = map[string]string{"name": "full_name"}
			table := makeTable("people", schema)
			row := makeRow(table, 1, "Alice")

			value, err := row.GetField("name")
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal("Alice"))

			Expect(row.SetField("name", "Bob")).To(Succeed())
			value, err = row.GetField("full_name")
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal("Bob"))
		})

		It("returns ErrFieldNotFound for missing fields", func() {
			schema := makeSchema(Column{Name: "id", Type: TypeInt})
			table := makeTable("people", schema)
			row := makeRow(table, 1)

			_, err := row.GetField("missing")
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(datamodel.ErrFieldNotFound.Error())))

			err = row.SetField("missing", 2)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(datamodel.ErrFieldNotFound.Error())))
		})
	})

	Describe("PrimaryKey", func() {
		It("returns an error when no PK is defined", func() {
			schema := makeSchema(Column{Name: "id", QualifiedName: "people.id", Type: TypeInt})
			table := makeTable("people", schema)
			row := makeRow(table, 1)

			_, err := row.PrimaryKey()
			Expect(err).To(HaveOccurred())
		})

		It("returns a deterministic key", func() {
			schema := makeSchema(
				Column{Name: "id", QualifiedName: "people.id", Type: TypeInt},
				Column{Name: "name", QualifiedName: "people.name", Type: TypeString},
			)
			schema.PKIndices = []int{0}
			table := makeTable("people", schema)
			row := makeRow(table, 1, "Alice")

			pk1, err := row.PrimaryKey()
			Expect(err).NotTo(HaveOccurred())
			pk2, err := row.PrimaryKey()
			Expect(err).NotTo(HaveOccurred())
			Expect(pk1).To(Equal(pk2))
		})
	})
})

var _ = Describe("Relational operators", func() {
	Describe("Product", func() {
		It("creates 2-way product with prefixed columns", func() {
			leftSchema := makeSchema(
				Column{Name: "id", Type: TypeInt},
				Column{Name: "name", Type: TypeString},
			)
			rightSchema := makeSchema(
				Column{Name: "dept", QualifiedName: "departments.dept", Type: TypeString},
			)
			leftTable := makeTable("employees", leftSchema)
			rightTable := makeTable("departments", rightSchema)

			leftRows := []datamodel.Document{
				makeRow(leftTable, 1, "Alice"),
				makeRow(leftTable, 2, "Bob"),
			}
			rightRows := []datamodel.Document{
				makeRow(rightTable, "eng"),
				makeRow(rightTable, "sales"),
			}

			result := product(leftRows, rightRows)
			Expect(result).To(HaveLen(4))

			row := rowAsRow(result[0])
			Expect(row.Table.Schema.Columns).To(HaveLen(3))
			Expect(row.Table.Schema.Columns[0].QualifiedName).To(Equal("employees.id"))
			Expect(row.Table.Schema.Columns[1].QualifiedName).To(Equal("employees.name"))
			Expect(row.Table.Schema.Columns[2].QualifiedName).To(Equal("departments.dept"))
		})

		It("creates 3-way product with repeated prefixes", func() {
			t1 := makeTable("t1", makeSchema(Column{Name: "id", Type: TypeInt}))
			t2 := makeTable("t2", makeSchema(Column{Name: "code", Type: TypeString}))
			t3 := makeTable("t3", makeSchema(Column{Name: "flag", Type: TypeString}))

			left := []datamodel.Document{makeRow(t1, 1)}
			middle := []datamodel.Document{makeRow(t2, "x")}
			right := []datamodel.Document{makeRow(t3, "y")}

			first := product(left, middle)
			final := product(first, right)
			Expect(final).To(HaveLen(1))

			row := rowAsRow(final[0])
			Expect(row.Table.Schema.Columns).To(HaveLen(3))
			Expect(row.Table.Schema.Columns[0].QualifiedName).To(Equal("t1.id"))
			Expect(row.Table.Schema.Columns[1].QualifiedName).To(Equal("t2.code"))
			Expect(row.Table.Schema.Columns[2].QualifiedName).To(Equal("t3.flag"))
		})
	})

	Describe("Join", func() {
		It("joins on matching values", func() {
			usersSchema := makeSchema(
				Column{Name: "id", Type: TypeInt},
				Column{Name: "name", Type: TypeString},
			)
			ordersSchema := makeSchema(
				Column{Name: "user_id", Type: TypeInt},
				Column{Name: "total", Type: TypeInt},
			)
			usersTable := makeTable("users", usersSchema)
			ordersTable := makeTable("orders", ordersSchema)

			users := []datamodel.Document{
				makeRow(usersTable, 1, "Alice"),
				makeRow(usersTable, 2, "Bob"),
			}
			orders := []datamodel.Document{
				makeRow(ordersTable, 1, 100),
				makeRow(ordersTable, 1, 120),
				makeRow(ordersTable, 3, 90),
			}

			result := joinOn(0, 0, users, orders)
			Expect(result).To(HaveLen(2))

			row := rowAsRow(result[0])
			Expect(row.Table.Schema.Columns).To(HaveLen(4))
			Expect(row.Table.Schema.Columns[0].QualifiedName).To(Equal("users.id"))
			Expect(row.Table.Schema.Columns[1].QualifiedName).To(Equal("users.name"))
			Expect(row.Table.Schema.Columns[2].QualifiedName).To(Equal("orders.user_id"))
			Expect(row.Table.Schema.Columns[3].QualifiedName).To(Equal("orders.total"))
		})

		It("joins three tables without double-prefixing", func() {
			usersSchema := makeSchema(
				Column{Name: "id", Type: TypeInt},
				Column{Name: "name", Type: TypeString},
			)
			ordersSchema := makeSchema(
				Column{Name: "user_id", Type: TypeInt},
				Column{Name: "total", Type: TypeInt},
			)
			paymentsSchema := makeSchema(
				Column{Name: "user_id", Type: TypeInt},
				Column{Name: "method", Type: TypeString},
			)
			usersTable := makeTable("users", usersSchema)
			ordersTable := makeTable("orders", ordersSchema)
			paymentsTable := makeTable("payments", paymentsSchema)

			users := []datamodel.Document{makeRow(usersTable, 1, "Alice")}
			orders := []datamodel.Document{makeRow(ordersTable, 1, 100)}
			payments := []datamodel.Document{makeRow(paymentsTable, 1, "card")}

			first := joinOn(0, 0, users, orders)
			final := joinOn(0, 0, first, payments)
			Expect(final).To(HaveLen(1))

			row := rowAsRow(final[0])
			Expect(row.Table.Schema.Columns).To(HaveLen(6))

			columnNames := make([]string, 0, len(row.Table.Schema.Columns))
			for _, col := range row.Table.Schema.Columns {
				columnNames = append(columnNames, col.QualifiedName)
			}
			Expect(strings.Count(columnNames[0], ".")).To(Equal(1))
			Expect(strings.Count(columnNames[1], ".")).To(Equal(1))
			Expect(strings.Count(columnNames[2], ".")).To(Equal(1))
			Expect(strings.Count(columnNames[3], ".")).To(Equal(1))
			Expect(strings.Count(columnNames[4], ".")).To(Equal(1))
			Expect(strings.Count(columnNames[5], ".")).To(Equal(1))
		})
	})
})
