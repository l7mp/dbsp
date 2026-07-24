package js

import (
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

var _ = Describe("Circuit builder", func() {
	Describe("parseOpSpec", func() {
		DescribeTable("resolves a node spec to an operator kind",
			func(spec any, kind string) {
				op, err := parseOpSpec(spec)
				Expect(err).NotTo(HaveOccurred())
				Expect(op.Kind().String()).To(Equal(kind))
			},
			// Type-only structural and primitive ops (bare string).
			Entry("input", "input", "input"),
			Entry("output", "output", "output"),
			Entry("delay", "delay", "delay"),
			Entry("integrate", "integrate", "integrate"),
			// Plus/Minus are LinearCombination instances ([1,1] / [1,-1]).
			Entry("plus", "plus", "linear_combination"),
			Entry("minus", "minus", "linear_combination"),
			Entry("negate", "negate", "negate"),
			Entry("distinct", "distinct", "distinct"),
			// Friendly aliases and the "@" prefix resolve to wire types.
			Entry("@int alias", "@int", "integrate"),
			Entry("@delta alias", "@delta", "delta0"),
			Entry("@distinct prefix", "@distinct", "distinct"),
			// Single-argument sugar.
			Entry("linear_combination coeffs", "linear_combination:[1,1]", "linear_combination"),
			Entry("@project projection", `@project:{"m":"$.n"}`, "project"),
			Entry("select predicate", `select:{"@eq":["$.a","$.b"]}`, "select"),
			Entry("unwind bare-string field", "unwind:$.items", "unwind"),
			// Multi-field operators take an object (or, for group_by, a pair).
			Entry("@groupBy pair", `@groupBy:["$.k","$."]`, "group_by"),
			// Wire-object form.
			Entry("object form", map[string]any{"type": "project", "projection": map[string]any{"m": "$.n"}}, "project"),
		)

		It("rejects an unknown operator", func() {
			_, err := parseOpSpec("nonesuch")
			Expect(err).To(HaveOccurred())
		})
	})

	It("builds, validates and runs a hand-made circuit", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("build-collector", vm.runtime, "cb-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = circuit.create("cb");
const i = c.input("cb-in");
const p = c.node('@project:{"m":"$.n"}');
const o = c.output("cb-out");
c.edge(i, p, 0);
c.edge(p, o, 0);
c.commit();
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			_ = runScript(vm, `publish("cb-in", [[{n: 42}, 1]]);`)
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(first.Name).To(Equal("cb-out"))
		Expect(zsetRowsByField(first, "m")).To(Equal([]string{"1:42"}))
	})

	It("validates without installing", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("val-collector", vm.runtime, "val-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		// .validate() reports the circuit well-formed but leaves it
		// offline, so the input goes nowhere until .commit() runs.
		script := `
const c = circuit.create("val");
const i = c.input("val-in");
const p = c.node('@project:{"m":"$.n"}');
const o = c.output("val-out");
c.edge(i, p, 0);
c.edge(p, o, 0);
c.validate();
publish("val-in", [[{n: 7}, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())
		Consistently(collector.Snapshot, 300*time.Millisecond, 50*time.Millisecond).Should(BeEmpty())

		Expect(runScript(vm, `c.commit();`)).To(Succeed())
		Eventually(collector.Snapshot, 2*time.Second, 10*time.Millisecond).ShouldNot(BeEmpty())
	})

	It("rejects a cycle with no delay when validated", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
const c = circuit.create("bad");
const a = c.node("plus", "a");
const b = c.node("negate", "b");
c.edge(a, b, 0);
c.edge(b, a, 0);
c.validate();
`
		err = runScript(vm, script)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cycle"))
	})
})
