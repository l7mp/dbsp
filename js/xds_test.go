package js

import (
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("xds global", func() {
	It("starts a named egress server and reports its address", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		Expect(runScript(vm, `
const s = xds.server.start({ address: "127.0.0.1:0" });
if (!s.address) { throw new Error("no address"); }
if (s.name !== "xds") { throw new Error("unexpected name: " + s.name); }
`)).To(Succeed())
	})

	It("binds an updater and a setter to the default server", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		Expect(runScript(vm, `
xds.server.start({ address: "127.0.0.1:0" });
xds.update("clusters",  { type: "cds" });
xds.set("listeners", { type: "lds" });
`)).To(Succeed())
	})

	It("requires the server to be started before binding a consumer", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `xds.update("clusters", { type: "cds" });`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not started"))
	})

	It("rejects an unknown xDS type", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `
xds.server.start({ address: "127.0.0.1:0" });
xds.update("x", { type: "bogus" });
`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unknown type"))
	})

	It("rejects a duplicate server name", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `
xds.server.start({ name: "edge", address: "127.0.0.1:0" });
xds.server.start({ name: "edge", address: "127.0.0.1:0" });
`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("already started"))
	})

	It("attaches delta and level watch producers against an upstream", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		// The producers dial lazily, so construction + Add succeed even though the
		// upstream here is just a loopback server with no data.
		Expect(runScript(vm, `
const s = xds.server.start({ address: "127.0.0.1:0" });
xds.watch("in-l", { type: "cds", address: s.address, level: true });
xds.watch("in-w", { type: "eds", address: s.address });
`)).To(Succeed())
	})
})
