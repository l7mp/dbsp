package xds

import (
	"context"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	resource "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// servedNames fetches the resources of a type over the given ADS variant and
// returns their names. It is the polling body behind Eventually: a context
// expiry (an as-yet-empty cache) reads back as an empty result rather than a
// failure.
func servedNames(fetch func(context.Context, string, []string) (map[string]proto.Message, error), typeURL string) func() []string {
	return func() []string {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		got, err := fetch(ctx, typeURL, nil)
		Expect(err).NotTo(HaveOccurred())
		names := make([]string, 0, len(got))
		for n := range got {
			names = append(names, n)
		}
		return names
	}
}

var _ = Describe("Consumer end to end", func() {
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

	// run wires a consumer for one type on topic, starts the server and the
	// consumer, and dials a test client at the server's address.
	run := func(topic, typ string) {
		c, err := srv.Updater(UpdaterConfig{Name: typ, OutputName: topic, Type: typ})
		Expect(err).NotTo(HaveOccurred())

		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		go func() { defer GinkgoRecover(); Expect(srv.Start(ctx)).To(Succeed()) }()
		go func() { defer GinkgoRecover(); Expect(c.Start(ctx)).To(Succeed()) }()

		client, err = dialTestClient(srv.Address())
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		rt = dbspruntime.NewRuntime("", logr.Discard())
		var err error
		srv, err = NewServer(ServerConfig{Address: "127.0.0.1:0", Runtime: rt})
		Expect(err).NotTo(HaveOccurred())
		pub = rt.NewPublisher()
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

	publish := func(topic string, z zset.ZSet) {
		Expect(pub.Publish(dbspruntime.Event{Name: topic, Data: z})).To(Succeed())
	}

	It("serves an inserted cluster over SotW ADS", func() {
		run("clusters", "cds")

		z := zset.New()
		z.Insert(cluster("c1", "STATIC"), 1)
		z.Insert(cluster("c2", "EDS"), 1)
		publish("clusters", z)

		Eventually(servedNames(client.fetchSotW, resource.ClusterType), 2*time.Second).
			Should(ConsistOf("c1", "c2"))
	})

	It("serves an inserted cluster over Delta ADS", func() {
		run("clusters", "cds")

		z := zset.New()
		z.Insert(cluster("c1", "STATIC"), 1)
		publish("clusters", z)

		Eventually(servedNames(client.fetchDelta, resource.ClusterType), 2*time.Second).
			Should(ConsistOf("c1"))
	})

	It("retracts a deleted cluster", func() {
		run("clusters", "cds")

		add := zset.New()
		add.Insert(cluster("c1", "STATIC"), 1)
		publish("clusters", add)

		Eventually(servedNames(client.fetchSotW, resource.ClusterType), 2*time.Second).
			Should(ConsistOf("c1"))

		del := zset.New()
		del.Insert(cluster("c1", "STATIC"), -1)
		publish("clusters", del)

		Eventually(servedNames(client.fetchSotW, resource.ClusterType), 2*time.Second).
			Should(BeEmpty())
	})

	It("preserves the served resource contents", func() {
		run("clusters", "cds")

		z := zset.New()
		z.Insert(cluster("c1", "EDS"), 1)
		publish("clusters", z)

		var got map[string]proto.Message
		Eventually(func() map[string]proto.Message {
			ctx, c := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer c()
			got, _ = client.fetchSotW(ctx, resource.ClusterType, nil)
			return got
		}, 2*time.Second).Should(HaveKey("c1"))

		Expect(got["c1"].(*clusterv3.Cluster).GetType()).To(Equal(clusterv3.Cluster_EDS))
	})

	It("serves endpoints keyed by cluster_name (EDS demo)", func() {
		run("endpoints", "eds")

		cla := unstructured.New(map[string]any{
			"clusterName": "cluster-a",
			"endpoints": []any{map[string]any{
				"lbEndpoints": []any{map[string]any{
					"endpoint": map[string]any{
						"address": map[string]any{
							"socketAddress": map[string]any{"address": "10.0.0.1", "portValue": 8080},
						},
					},
				}},
			}},
		})
		z := zset.New()
		z.Insert(cla, 1)
		publish("endpoints", z)

		var got map[string]proto.Message
		Eventually(func() map[string]proto.Message {
			ctx, c := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer c()
			got, _ = client.fetchSotW(ctx, resource.EndpointType, nil)
			return got
		}, 2*time.Second).Should(HaveKey("cluster-a"))

		assignment := got["cluster-a"].(*endpointv3.ClusterLoadAssignment)
		Expect(assignment.GetEndpoints()).To(HaveLen(1))
	})
})
