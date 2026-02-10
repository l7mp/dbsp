package relation

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/zset"
)

func TestRelationTable(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Relation Table Suite")
}

type badDoc struct {
	id string
}

func (b badDoc) Hash() string { return b.id }
func (b badDoc) PrimaryKey() (string, error) {
	return b.id, nil
}
func (b badDoc) String() string { return b.id }
func (b badDoc) Concat(other datamodel.Document) datamodel.Document {
	return b
}
func (b badDoc) Copy() datamodel.Document { return b }
func (b badDoc) GetField(_ string) (any, error) {
	return nil, datamodel.ErrFieldNotFound
}
func (b badDoc) SetField(_ string, _ any) error {
	return datamodel.ErrFieldNotFound
}

var _ = Describe("Table/ZSet conversion", func() {
	It("round-trips Table -> ZSet -> Table", func() {
		schema := &Schema{
			Columns: []Column{
				{Name: "id", Type: TypeInt},
				{Name: "name", Type: TypeString},
				{Name: "age", Type: TypeInt},
			},
			PKIndices: []int{0},
		}

		original := NewTable("users", schema)
		Expect(original.Insert([]any{1, "Alice", 30})).To(Succeed())
		Expect(original.Insert([]any{2, "Bob", 45})).To(Succeed())

		z := original.ToZSet()

		copy := NewTable("users_copy", schema)
		Expect(ZSetToTable(z, copy)).To(Succeed())

		z2 := copy.ToZSet()
		Expect(z2.Equal(z)).To(BeTrue())
	})

	It("rejects non-row documents", func() {
		schema := &Schema{
			Columns: []Column{
				{Name: "id", Type: TypeInt},
			},
			PKIndices: []int{0},
		}

		table := NewTable("bad", schema)
		z := zset.New()
		z.Insert(badDoc{id: "nope"}, 1)

		err := ZSetToTable(z, table)
		Expect(err).To(HaveOccurred())
	})

	It("claims rows into a new table", func() {
		schema := &Schema{
			Columns: []Column{
				{Name: "id", QualifiedName: "users.id", Type: TypeInt},
				{Name: "name", QualifiedName: "users.name", Type: TypeString},
			},
			PKIndices: []int{0},
		}

		rowTable := NewTable("users", schema)
		row := &Row{Table: rowTable, Data: []any{1, "Alice"}}

		newTable := NewTable("users_copy", schema)
		claimed, err := newTable.Claim(row)
		Expect(err).NotTo(HaveOccurred())
		Expect(claimed.Table).To(Equal(newTable))
		Expect(claimed.Data).To(Equal(row.Data))
	})

	It("errors on column count mismatch", func() {
		schema := &Schema{
			Columns: []Column{
				{Name: "id", Type: TypeInt},
				{Name: "name", Type: TypeString},
			},
			PKIndices: []int{0},
		}

		z := zset.New()
		z.Insert(&Row{Table: NewTable("users", schema), Data: []any{1}}, 1)

		table := NewTable("users", schema)
		err := ZSetToTable(z, table)
		Expect(err).To(HaveOccurred())
	})
})
