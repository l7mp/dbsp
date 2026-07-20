package aggregation

import (
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/internal/logger"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

var _ = Describe("Indexed equi-join (@join index option)", func() {
	const plainHard = `[
		{"@join": {"@eq": ["$.l.spec.ref", "$.r.metadata.name"]}},
		{"@project": {"metadata": {"name": {"@concat": ["$.l.metadata.name", "-", "$.r.metadata.name"]}, "namespace": "default"}, "l": "$.l.metadata.name", "r": "$.r.metadata.name"}}
	]`
	const indexedHard = `[
		{"@join": [{"@eq": ["$.l.spec.ref", "$.r.metadata.name"]}, {"index": {"l": "$.spec.ref", "r": "$.metadata.name"}}]},
		{"@project": {"metadata": {"name": {"@concat": ["$.l.metadata.name", "-", "$.r.metadata.name"]}, "namespace": "default"}, "l": "$.l.metadata.name", "r": "$.r.metadata.name"}}
	]`
	const plainSoft = `[
		{"@join": [{"@eq": ["$.l.spec.ref", "$.r.metadata.name"]}, {"soft": ["r"]}]},
		{"@project": {"metadata": {"name": {"@concat": ["$.l.metadata.name", "-", {"@definedOr": ["$.r.metadata.name", "none"]}]}, "namespace": "default"}, "l": "$.l.metadata.name", "r": {"@definedOr": ["$.r.metadata.name", ""]}}}
	]`
	const indexedSoft = `[
		{"@join": [{"@eq": ["$.l.spec.ref", "$.r.metadata.name"]}, {"soft": ["r"], "index": {"l": "$.spec.ref", "r": "$.metadata.name"}}]},
		{"@project": {"metadata": {"name": {"@concat": ["$.l.metadata.name", "-", {"@definedOr": ["$.r.metadata.name", "none"]}]}, "namespace": "default"}, "l": "$.l.metadata.name", "r": {"@definedOr": ["$.r.metadata.name", ""]}}}
	]`

	left := func(name, ref string) *unstructured.Unstructured {
		return unstructured.New(map[string]any{
			"metadata": map[string]any{"name": name},
			"spec":     map[string]any{"ref": ref},
		})
	}
	right := func(name string) *unstructured.Unstructured {
		return unstructured.New(map[string]any{
			"metadata": map[string]any{"name": name},
		})
	}

	entry := func(doc *unstructured.Unstructured, w zset.Weight) zset.Elem {
		return zset.Elem{Document: doc, Weight: w}
	}
	deltas := func(entries ...zset.Elem) zset.ZSet {
		z := zset.New()
		for _, e := range entries {
			z.Insert(e.Document, e.Weight)
		}
		return z
	}

	// steps is a delta sequence exercising inserts on both sides, an update
	// (retract + assert) and a full retraction.
	steps := []map[string]zset.ZSet{
		{"input_l": deltas(entry(left("l1", "a"), 1), entry(left("l2", "b"), 1)), "input_r": deltas(entry(right("a"), 1))},
		{"input_r": deltas(entry(right("b"), 1), entry(right("c"), 1))},
		{"input_l": deltas(entry(left("l1", "a"), -1), entry(left("l1", "c"), 1))},
		{"input_r": deltas(entry(right("b"), -1))},
		{"input_l": deltas(entry(left("l2", "b"), -1))},
	}

	compile := func(pipeline string) *executor.Executor {
		GinkgoHelper()
		c := New(toIdentityBindings([]string{"l", "r"}), toIdentityBindings([]string{"output"}))
		q, err := c.CompileString(pipeline)
		Expect(err).NotTo(HaveOccurred())
		reg, err := transform.NewRegularizer().Transform(q.Circuit)
		Expect(err).NotTo(HaveOccurred())
		incr, err := transform.NewIncrementalizer().Transform(reg)
		Expect(err).NotTo(HaveOccurred())
		exec, err := executor.New(incr, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())
		return exec
	}

	// integrate runs the incremental executor over the delta sequence and
	// returns the integrated output state.
	integrate := func(exec *executor.Executor) zset.ZSet {
		GinkgoHelper()
		acc := zset.New()
		for _, step := range steps {
			outs, err := exec.Execute(step)
			Expect(err).NotTo(HaveOccurred())
			for _, out := range outs {
				acc = acc.Add(out)
			}
		}
		return acc
	}

	It("compiles the index option into an equi-join node", func() {
		c := New(toIdentityBindings([]string{"l", "r"}), toIdentityBindings([]string{"output"}))
		q, err := c.CompileString(indexedHard)
		Expect(err).NotTo(HaveOccurred())
		found := false
		for _, n := range q.Circuit.Nodes() {
			if n.Operator != nil && n.Operator.Kind().String() == "equi_join" {
				found = true
			}
		}
		Expect(found).To(BeTrue(), "expected an equi_join node in the compiled circuit")
	})

	It("rejects index entries that are not join participants", func() {
		c := New(toIdentityBindings([]string{"l", "r"}), toIdentityBindings([]string{"output"}))
		_, err := c.CompileString(`[
			{"@join": [true, {"index": {"l": "$.spec.ref", "bogus": "$.metadata.name"}}]},
			{"@project": {"$.": "$."}}
		]`)
		Expect(err).To(HaveOccurred())
	})

	It("computes the same incremental hard-join results as the plain join", func() {
		plain := integrate(compile(plainHard))
		indexed := integrate(compile(indexedHard))
		Expect(indexed.Equal(plain)).To(BeTrue(),
			"indexed: %s\nplain: %s", indexed.String(), plain.String())
		// Sanity: after the update step l1 joins c and l2 was retracted.
		Expect(plain.Size()).To(Equal(1))
	})

	It("computes the same incremental soft-join results as the plain soft join", func() {
		plain := integrate(compile(plainSoft))
		indexed := integrate(compile(indexedSoft))
		Expect(indexed.Equal(plain)).To(BeTrue(),
			"indexed: %s\nplain: %s", indexed.String(), plain.String())
		// Sanity: the surviving left row joined its match.
		matched := false
		plain.Iter(func(doc datamodel.Document, w zset.Weight) bool {
			if name, err := doc.GetField("$.metadata.name"); err == nil && name == "l1-c" {
				matched = w > 0
				return false
			}
			return true
		})
		Expect(matched).To(BeTrue(), "expected l1-c in %s", plain.String())
	})
})
