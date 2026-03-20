package relation

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Database DML", func() {
	It("inserts, updates, and deletes rows", func() {
		db := NewDatabase("test")
		schema := NewSchema("id", "name").WithQualifiedNames("t").WithPrimaryKey(0)
		table := NewTable("t", schema)
		db.RegisterTable("t", table)

		Expect(db.Insert("t", []any{int64(1), "alice"})).To(Succeed())
		pk := mustPrimaryKey(table, []any{int64(1), "alice"})
		row, ok := table.Lookup(pk)
		Expect(ok).To(BeTrue())
		Expect(row.Data[1]).To(Equal("alice"))

		updated, err := db.Update("t", pk, func(r *Row) error {
			return r.SetField("name", "bob")
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(updated.Data[1]).To(Equal("bob"))

		deleted, err := db.Delete("t", pk)
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())
	})
})

func mustPrimaryKey(t *Table, data []any) string {
	row := &Row{Table: t, Data: data}
	pk, err := row.PrimaryKey()
	Expect(err).NotTo(HaveOccurred())
	return pk
}
