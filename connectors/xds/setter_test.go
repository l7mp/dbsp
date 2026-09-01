package xds

import (
	"context"
	"time"

	resource "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Setter end to end", func() {
	var (
		rt     *dbspruntime.Runtime
		srv    *Server
		client *testClient
		cancel context.CancelFunc
		pub    dbspruntime.Publisher
	)

	cluster := func(name, typ string) *unstructured.Unstructured {
		return unstructured.New(map[string]any{"name": name, "type": typ})
	}

	BeforeEach(func() {
		rt = dbspruntime.NewRuntime("", logr.Discard())
		var err error
		srv, err = NewServer(ServerConfig{Address: "127.0.0.1:0", Runtime: rt})
		Expect(err).NotTo(HaveOccurred())
		pub = rt.NewPublisher()

		s, err := srv.Setter(SetterConfig{Name: "cds", OutputName: "clusters", Type: "cds"})
		Expect(err).NotTo(HaveOccurred())

		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		go func() { defer GinkgoRecover(); Expect(srv.Start(ctx)).To(Succeed()) }()
		go func() { defer GinkgoRecover(); Expect(s.Start(ctx)).To(Succeed()) }()

		client, err = dialTestClient(srv.Address())
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if client != nil {
			Expect(client.Close()).To(Succeed())
			client = nil
		}
		if cancel != nil {
			cancel()
			cancel = nil
		}
	})

	// snapshot publishes a full desired set as an all-positive Z-set.
	snapshot := func(clusters ...*unstructured.Unstructured) {
		z := zset.New()
		for _, c := range clusters {
			z.Insert(c, 1)
		}
		Expect(pub.Publish(dbspruntime.Event{Name: "clusters", Data: z})).To(Succeed())
	}

	It("serves the full snapshot", func() {
		snapshot(cluster("c1", "STATIC"), cluster("c2", "EDS"))
		Eventually(servedNames(client.fetchSotW, resource.ClusterType), 2*time.Second).
			Should(ConsistOf("c1", "c2"))
	})

	It("removes an omitted resource on the next snapshot (delete by omission)", func() {
		snapshot(cluster("c1", "STATIC"), cluster("c2", "EDS"))
		Eventually(servedNames(client.fetchSotW, resource.ClusterType), 2*time.Second).
			Should(ConsistOf("c1", "c2"))

		snapshot(cluster("c1", "STATIC")) // c2 omitted -> dropped
		Eventually(servedNames(client.fetchSotW, resource.ClusterType), 2*time.Second).
			Should(ConsistOf("c1"))
	})
})
