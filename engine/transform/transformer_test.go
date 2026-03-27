package transform

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Transformer factory", func() {
	It("creates Incrementalizer", func() {
		t, err := New(Incrementalizer)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Incrementalizer))
	})

	It("creates Rewriter with default rules", func() {
		t, err := New(Rewriter)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Rewriter))
	})

	It("creates Rewriter with named rule set", func() {
		t, err := New(Rewriter, "Post")
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Rewriter))
	})

	It("creates Reconciler with explicit pairs", func() {
		pairs := []ReconcilerPair{{InputID: "in", OutputID: "out"}}
		t, err := New(Reconciler, pairs)
		Expect(err).NotTo(HaveOccurred())
		Expect(t.Name()).To(Equal(Reconciler))
	})

	It("rejects unknown transformer", func() {
		_, err := New(TransformerType("Bogus"))
		Expect(err).To(HaveOccurred())
	})
})
