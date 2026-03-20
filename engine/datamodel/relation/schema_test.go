package relation

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Schema", func() {
	Describe("AliasForColumn", func() {
		It("returns false when aliases are nil", func() {
			schema := &Schema{}
			alias, ok := schema.AliasForColumn("id")
			Expect(ok).To(BeFalse())
			Expect(alias).To(BeEmpty())
		})

		It("resolves aliases case-insensitively", func() {
			schema := &Schema{
				Aliases: map[string]string{
					"name": "full_name",
				},
			}
			alias, ok := schema.AliasForColumn("NAME")
			Expect(ok).To(BeTrue())
			Expect(alias).To(Equal("full_name"))
		})
	})
})
