package relation

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Database", func() {
	It("registers and retrieves tables case-insensitively", func() {
		db := NewDatabase("test")
		schema := &Schema{Columns: []Column{{Name: "id", Type: TypeInt}}}
		table := NewTable("Users", schema)

		db.RegisterTable("Users", table)

		fetched, err := db.GetTable("users")
		Expect(err).NotTo(HaveOccurred())
		Expect(fetched).To(Equal(table))
	})

	It("returns an error for missing tables", func() {
		db := NewDatabase("test")
		_, err := db.GetTable("missing")
		Expect(err).To(HaveOccurred())
	})
})
