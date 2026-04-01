package main

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CLI args", func() {
	It("passes script arguments into process.argv", func() {
		tmpDir, err := os.MkdirTemp("", "dbsp-cli-argv-")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(os.RemoveAll(tmpDir)).To(Succeed())
		})

		scriptPath := filepath.Join(tmpDir, "argv.js")
		script := "\nconst args = (process && process.argv) ? process.argv.slice(2) : [];\nif (args.join(\",\") !== \"test,gwclass\") {\n  throw new Error(\"unexpected argv: \" + JSON.stringify(args));\n}\n"
		Expect(os.WriteFile(scriptPath, []byte(script), 0o600)).To(Succeed())

		err = run([]string{scriptPath, "test", "gwclass"})
		Expect(err).NotTo(HaveOccurred())
	})
})
