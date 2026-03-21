package main

import (
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
)

var _ = Describe("Example scripts", func() {
	It("runs join_project.js without errors", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		done := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			done <- vm.RunFile(filepath.Join("examples", "join_project.js"))
		}()

		time.AfterFunc(200*time.Millisecond, vm.Close)
		Eventually(done).Should(Receive(BeNil()))
	})
})
