package js

import (
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("kubernetes.expression namespace", func() {
	It("registers a connector operator and evaluates it in a pipeline", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("selmatch-collector", vm.runtime, "selmatch-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
kubernetes.expression.register("@selectorMatches");

const c = aggregate.compile([
  {"@select": {"@selectorMatches": [{matchLabels: {app: "web"}}, "$.labels"]}}
], {
  inputs: "selmatch-in",
  outputs: ["selmatch-out"]
});
c.transform({ name: "Incrementalizer" }).commit();
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			if err := runScript(vm, `publish("selmatch-in", [
  [{id: "yes", labels: {app: "web"}}, 1],
  [{id: "no", labels: {app: "db"}}, 1],
]);`); err != nil {
				return false
			}
			return len(collector.Snapshot()) > 0
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		entries := collector.Snapshot()[0].Data.Entries()
		Expect(entries).To(HaveLen(1))
		id, err := entries[0].Document.GetField("$.id")
		Expect(err).NotTo(HaveOccurred())
		Expect(id).To(Equal("yes"))
	})

	It("lists the offered operators and rejects unknown names", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		Expect(runScript(vm, `
const names = kubernetes.expression.list();
if (!names.includes("@selectorMatches")) {
  throw new Error("missing @selectorMatches: " + JSON.stringify(names));
}
`)).To(Succeed())

		err = runScript(vm, `kubernetes.expression.register("@nonesuch");`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unknown operator"))
		Expect(err.Error()).To(ContainSubstring("@selectorMatches"))
	})

	It("refuses registration and unregistration after the first compile", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		Expect(runScript(vm, `
const c = aggregate.compile([{"@project": {"id": "$.id"}}], {
  inputs: "gate-in",
  outputs: ["gate-out"]
});
`)).To(Succeed())

		err = runScript(vm, `kubernetes.expression.register("@selectorMatches");`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("init phase"))

		err = runScript(vm, `expression.register("@late", (x) => x);`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("init phase"))

		err = runScript(vm, `expression.unregister("@late");`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("init phase"))
	})

	It("guards connector names against the JS registrant and back", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		// Held by the connector: the JS registrant cannot take it over.
		Expect(runScript(vm, `kubernetes.expression.register("@selectorMatches");`)).To(Succeed())
		err = runScript(vm, `expression.register("@selectorMatches", () => true);`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`already registered by "kubernetes"`))

		// The JS registrant cannot unregister it either; the connector
		// namespace can, after which the name is free for a JS variant.
		err = runScript(vm, `expression.unregister("@selectorMatches");`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`registered by "kubernetes"`))

		Expect(runScript(vm, `
kubernetes.expression.unregister("@selectorMatches");
expression.register("@selectorMatches", () => true);
expression.unregister("@selectorMatches");
`)).To(Succeed())
	})
})
