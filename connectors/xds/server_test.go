package xds

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

var _ = Describe("Server", func() {
	var rt *dbspruntime.Runtime

	BeforeEach(func() {
		rt = dbspruntime.NewRuntime(logr.Discard())
	})

	It("opens a listener and reports its resolved address", func() {
		srv, err := NewServer(ServerConfig{Address: "127.0.0.1:0", Runtime: rt})
		Expect(err).NotTo(HaveOccurred())
		Expect(srv.Address()).NotTo(BeEmpty())
		Expect(srv.Name()).To(Equal("xds"))
	})

	It("honours an explicit name", func() {
		srv, err := NewServer(ServerConfig{Name: "edge", Address: "127.0.0.1:0", Runtime: rt})
		Expect(err).NotTo(HaveOccurred())
		Expect(srv.Name()).To(Equal("edge"))
	})

	It("serves until the context is cancelled", func() {
		srv, err := NewServer(ServerConfig{Address: "127.0.0.1:0", Runtime: rt})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- srv.Start(ctx) }()

		time.Sleep(50 * time.Millisecond) // let the gRPC server begin serving
		cancel()
		Eventually(done, time.Second).Should(Receive(BeNil()))
	})

	Describe("Consumer", func() {
		var srv *Server

		BeforeEach(func() {
			var err error
			srv, err = NewServer(ServerConfig{Address: "127.0.0.1:0", Runtime: rt})
			Expect(err).NotTo(HaveOccurred())
		})

		It("builds a consumer for a known type", func() {
			c, err := srv.Updater(UpdaterConfig{Name: "cds", OutputName: "clusters", Type: "cds"})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.Name()).To(Equal("cds"))
		})

		It("rejects an unknown type", func() {
			_, err := srv.Updater(UpdaterConfig{Name: "x", OutputName: "t", Type: "bogus"})
			Expect(err).To(HaveOccurred())
		})
	})
})
