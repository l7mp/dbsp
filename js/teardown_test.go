package js

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("circuit.close()", func() {
	It("is idempotent and lets the circuit be re-validated", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], { inputs: "tdn-in", outputs: ["tdn-out"] });
c.validate();
c.close();
c.close();
c.validate();
`
		Expect(runScript(vm, script)).To(Succeed())
	})

	It("clears an active observer when closed", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], { inputs: "tdn-obs-in", outputs: ["tdn-obs-out"] });
c.validate();
c.observe(() => {});
c.close();
`
		Expect(runScript(vm, script)).To(Succeed())
	})
})

var _ = Describe("kubernetes.runtime", func() {
	It("requires explicit start before kubernetes.watch", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `kubernetes.watch("services", {gvk: "v1/Service"});`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("kubernetes.runtime.start()"))
	})

	It("allows idempotent start with equivalent configs", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `
kubernetes.runtime.start();
kubernetes.runtime.start({});
`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("generates key material without starting runtime", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		tmp := GinkgoT().TempDir()
		keyFile := filepath.Join(tmp, "apiserver.key")
		certFile := filepath.Join(tmp, "apiserver.crt")

		script := fmt.Sprintf(`
const cfg = kubernetes.runtime.config();
cfg.generateKeys({
  hostnames: ["localhost"],
  keyFile: %q,
  certFile: %q,
});
`, keyFile, certFile)
		Expect(runScript(vm, script)).To(Succeed())

		_, err = os.Stat(keyFile)
		Expect(err).NotTo(HaveOccurred())
		_, err = os.Stat(certFile)
		Expect(err).NotTo(HaveOccurred())
	})

	It("generates kubeconfig from runtime config without start", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		tmp := GinkgoT().TempDir()
		keyFile := filepath.Join(tmp, "apiserver.key")
		certFile := filepath.Join(tmp, "apiserver.crt")

		script := fmt.Sprintf(`
const cfg = kubernetes.runtime.config({
  apiServer: {addr: "127.0.0.1", port: 8443, http: true},
});
cfg.generateKeys({
  hostnames: ["127.0.0.1", "localhost"],
  keyFile: %q,
  certFile: %q,
});
const y = cfg.generateKubeConfig({
  user: "alice",
  keyFile: %q,
  http: true,
  serverAddress: "127.0.0.1:8443",
});
if (!String(y).includes("alice")) {
  throw new Error("expected generated kubeconfig output");
}
`, keyFile, certFile, keyFile)
		Expect(runScript(vm, script)).To(Succeed())
	})
})
