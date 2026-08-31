package transform

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TransformSpec", func() {
	It("parses the wire entries into Specs", func() {
		s, err := TransformSpec{Name: "Incrementalizer"}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Type).To(Equal(Incrementalizer))
		Expect(s.Args).To(BeEmpty())

		s, err = TransformSpec{Name: "Reconciler", Pairs: [][]string{{"observed", "status"}}}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Type).To(Equal(Reconciler))
		Expect(s.Args).To(HaveLen(1))
		Expect(s.Args[0]).To(Equal([]ReconcilerPair{{InputID: "input_observed", OutputID: "output_status"}}))

		// Node IDs pass through uncanonicalzed.
		s, err = TransformSpec{Name: "Reconciler", Pairs: [][]string{{"input_a", "output_b"}}}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Args[0]).To(Equal([]ReconcilerPair{{InputID: "input_a", OutputID: "output_b"}}))

		s, err = TransformSpec{Name: "SmithPredictor", Pairs: [][]string{{"o", "s"}}, K: 3}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Type).To(Equal(SmithPredictor))
		Expect(s.Args).To(HaveLen(2))
		Expect(s.Args[1]).To(Equal(3))

		s, err = TransformSpec{Name: "Distincter", Key: json.RawMessage(`"$.metadata.name"`)}.Spec()
		Expect(err).NotTo(HaveOccurred())
		Expect(s.Type).To(Equal(Distincter))
		Expect(s.Args).To(HaveLen(1))
	})

	It("rejects malformed entries", func() {
		_, err := TransformSpec{}.Spec()
		Expect(err).To(MatchError(ContainSubstring("missing transformer name")))
		_, err = TransformSpec{Name: "NoSuch"}.Spec()
		Expect(err).To(MatchError(ContainSubstring("unknown transformer")))
		_, err = TransformSpec{Name: "Rewriter"}.Spec()
		Expect(err).To(MatchError(ContainSubstring("not a user-facing transform")))
		_, err = TransformSpec{Name: "Reconciler", Pairs: [][]string{{"only-one"}}}.Spec()
		Expect(err).To(MatchError(ContainSubstring("exactly 2 elements")))
		_, err = TransformSpec{Name: "Reconciler", Pairs: [][]string{{"", "b"}}}.Spec()
		Expect(err).To(MatchError(ContainSubstring("empty values")))
		_, err = TransformSpec{Name: "Distincter", Key: json.RawMessage(`{"@nosuch": 1}`)}.Spec()
		Expect(err).To(MatchError(ContainSubstring("key")))
	})

	It("builds a canonical chain from wire entries", func() {
		ch, err := NewChainFromSpecs([]TransformSpec{
			{Name: "Incrementalizer"},
			{Name: "Reconciler", Pairs: [][]string{{"o", "s"}}},
		})
		Expect(err).NotTo(HaveOccurred())
		specs := ch.Specs()
		Expect(specs[0].Type).To(Equal(Reconciler))
		Expect(specs[1].Type).To(Equal(Incrementalizer))

		_, err = NewChainFromSpecs([]TransformSpec{{Name: "Incrementalizer"}, {Name: "Incrementalizer"}})
		Expect(err).To(MatchError(ContainSubstring("listed twice")))
	})

	It("round-trips through JSON", func() {
		src := `{"name":"SmithPredictor","pairs":[["o","s"]],"k":2}`
		var ts TransformSpec
		Expect(json.Unmarshal([]byte(src), &ts)).To(Succeed())
		b, err := json.Marshal(ts)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(MatchJSON(src))
	})
})
