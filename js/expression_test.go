package js

import (
	"fmt"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("JS expression operators", func() {
	It("evaluates a registered operator inside a compiled pipeline", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("expr-collector", vm.runtime, "expr-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
expression.register("@double", (x) => x * 2);

const c = aggregate.compile([
  {"@project": {"id": "$.id", "value": {"@double": "$.value"}}}
], {
  inputs: "expr-in",
  outputs: ["expr-out"]
});
c.transform({ name: "Incrementalizer" });
c.validate();
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			if err := runScript(vm, `publish("expr-in", [[{id: 1, value: 21}, 1]]);`); err != nil {
				return false
			}
			return len(collector.Snapshot()) > 0
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		entries := collector.Snapshot()[0].Data.Entries()
		Expect(entries).NotTo(BeEmpty())
		value, err := entries[0].Document.GetField("$.value")
		Expect(err).NotTo(HaveOccurred())
		Expect(fmt.Sprint(value)).To(Equal("42"))
	})

	It("supports list-valued arguments and results (ordering by another list)", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("order-collector", vm.runtime, "order-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
// Order the items list by the position of item[key] in the order list;
// items not named in the order list go last, in stable order.
expression.register("@orderBy", (items, order, key) => {
  const pos = new Map(order.map((n, i) => [n, i]));
  return items
    .map((item, i) => [item, pos.has(item[key]) ? pos.get(item[key]) : order.length + i])
    .sort((a, b) => a[1] - b[1])
    .map(([item]) => item);
});

const c = aggregate.compile([
  {"@project": {"id": "$.id", "sorted": {"@orderBy": ["$.items", "$.order", "name"]}}}
], {
  inputs: "order-in",
  outputs: ["order-out"]
});
c.transform({ name: "Incrementalizer" });
c.validate();
`
		Expect(runScript(vm, script)).To(Succeed())

		publish := `
publish("order-in", [[{
  id: 1,
  items: [{name: "c"}, {name: "a"}, {name: "b"}],
  order: ["a", "b", "c"]
}, 1]]);
`
		Eventually(func() bool {
			if err := runScript(vm, publish); err != nil {
				return false
			}
			return len(collector.Snapshot()) > 0
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		entries := collector.Snapshot()[0].Data.Entries()
		Expect(entries).NotTo(BeEmpty())
		sorted, err := entries[0].Document.GetField("$.sorted")
		Expect(err).NotTo(HaveOccurred())
		list, ok := sorted.([]any)
		Expect(ok).To(BeTrue(), "sorted should be a list, got %T", sorted)
		names := make([]string, 0, len(list))
		for _, item := range list {
			m, ok := item.(map[string]any)
			Expect(ok).To(BeTrue())
			names = append(names, fmt.Sprint(m["name"]))
		}
		Expect(names).To(Equal([]string{"a", "b", "c"}))
	})

	It("propagates JS callback errors as expression errors", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("boom-collector", vm.runtime, "boom-err")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
expression.register("@boom", () => { throw new Error("kaboom"); });

runtime.onError((e) => {
  publish("boom-err", [[{origin: e.origin, message: e.message}, 1]]);
});

const c = aggregate.compile([
  {"@project": {"id": {"@boom": "$.id"}}}
], {
  inputs: "boom-in",
  outputs: ["boom-out"]
});
c.transform({ name: "Incrementalizer" });
`
		Expect(runScript(vm, script)).To(Succeed())

		Expect(runScript(vm, `publish("boom-in", [[{id: 1}, 1]]);`)).To(Succeed())

		Eventually(func() bool {
			for _, e := range collector.Snapshot() {
				for _, entry := range e.Data.Entries() {
					if msg, err := entry.Document.GetField("$.message"); err == nil &&
						fmt.Sprint(msg) != "" {
						return fmt.Sprint(msg) != ""
					}
				}
			}
			return false
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		msgs := ""
		for _, e := range collector.Snapshot() {
			for _, entry := range e.Data.Entries() {
				if msg, err := entry.Document.GetField("$.message"); err == nil {
					msgs += fmt.Sprint(msg)
				}
			}
		}
		Expect(msgs).To(ContainSubstring("kaboom"))
	})

	It("rejects non-function callbacks and built-in names", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `expression.register("@nofn", 42);`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("must be a function"))

		err = runScript(vm, `expression.register("@add", () => 0);`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("built-in"))
	})
})
