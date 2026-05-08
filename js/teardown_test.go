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

	It("resolves view GVK from operator and kind", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
const gvk = kubernetes.runtime.resolveGVK({
  operator: "demo",
  kind: "Widget"
});
if (gvk.group !== "demo.view.dcontroller.io") {
  throw new Error("unexpected group: " + gvk.group);
}
if (gvk.version !== "v1alpha1") {
  throw new Error("unexpected version: " + gvk.version);
}
if (gvk.kind !== "Widget") {
  throw new Error("unexpected kind: " + gvk.kind);
}
`
		Expect(runScript(vm, script)).To(Succeed())
	})

	It("resolves explicit core and grouped GVK values", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
const core = kubernetes.runtime.resolveGVK({
  apiGroup: "",
  kind: "Service"
});
if (core.group !== "" || core.version !== "v1" || core.kind !== "Service") {
  throw new Error("unexpected core resolution");
}

const apps = kubernetes.runtime.resolveGVK({
  apiGroup: "apps",
  version: "v1",
  kind: "Deployment"
});
if (apps.gvk !== "apps/v1/Deployment") {
  throw new Error("unexpected apps resolution: " + apps.gvk);
}
`
		Expect(runScript(vm, script)).To(Succeed())
	})

	It("requires started runtime for native group resolution without version", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `kubernetes.runtime.resolveGVK({apiGroup: "apps", kind: "Deployment"});`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("kubernetes.runtime.start()"))
	})

	It("registers and unregisters view GVKs", func() {
		oldKubeconfig, hadKubeconfig := os.LookupEnv("KUBECONFIG")
		Expect(os.Unsetenv("KUBECONFIG")).To(Succeed())
		DeferCleanup(func() {
			if hadKubeconfig {
				Expect(os.Setenv("KUBECONFIG", oldKubeconfig)).To(Succeed())
				return
			}
			Expect(os.Unsetenv("KUBECONFIG")).To(Succeed())
		})

		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
kubernetes.runtime.start();
kubernetes.runtime.registerViews([
  "demo.view.dcontroller.io/v1alpha1/Widget"
]);
kubernetes.runtime.unregisterViews({
  gvks: ["demo.view.dcontroller.io/v1alpha1/Widget"]
});
`
		Expect(runScript(vm, script)).To(Succeed())
	})

	It("rejects non-view GVK registration", func() {
		oldKubeconfig, hadKubeconfig := os.LookupEnv("KUBECONFIG")
		Expect(os.Unsetenv("KUBECONFIG")).To(Succeed())
		DeferCleanup(func() {
			if hadKubeconfig {
				Expect(os.Setenv("KUBECONFIG", oldKubeconfig)).To(Succeed())
				return
			}
			Expect(os.Unsetenv("KUBECONFIG")).To(Succeed())
		})

		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		Expect(runScript(vm, `kubernetes.runtime.start();`)).To(Succeed())

		err = runScript(vm, `kubernetes.runtime.registerViews(["v1/Pod"]);`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("is not a view group"))
	})

	It("uses explicit kubeconfig path when provided", func() {
		oldKubeconfig, hadKubeconfig := os.LookupEnv("KUBECONFIG")
		Expect(os.Setenv("KUBECONFIG", "/tmp/unused-kubeconfig")).To(Succeed())
		DeferCleanup(func() {
			if hadKubeconfig {
				Expect(os.Setenv("KUBECONFIG", oldKubeconfig)).To(Succeed())
				return
			}
			Expect(os.Unsetenv("KUBECONFIG")).To(Succeed())
		})

		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		missing := filepath.Join(GinkgoT().TempDir(), "missing.kubeconfig")
		script := fmt.Sprintf(`kubernetes.runtime.start({kubeconfig: %q});`, missing)
		err = runScript(vm, script)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("load kubeconfig %q", missing)))
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
