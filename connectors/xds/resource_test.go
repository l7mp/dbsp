package xds

import (
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	// Linked so protojson can resolve the @type discriminators used in
	// typed_config below.
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"

	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
)

func doc(fields map[string]any) *unstructured.Unstructured {
	return unstructured.New(fields)
}

func entryFor(id string) typeEntry {
	e, ok := lookupType(id)
	Expect(ok).To(BeTrue(), "unknown type id %q", id)
	return e
}

var _ = Describe("decodeDocument", func() {
	It("decodes a CDS document, parsing enums and durations", func() {
		res, name, err := decodeDocument(entryFor("cds"), doc(map[string]any{
			"name":           "cluster-a",
			"type":           "STATIC",
			"connectTimeout": "5s",
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(name).To(Equal("cluster-a"))

		cl, ok := res.(*clusterv3.Cluster)
		Expect(ok).To(BeTrue())
		Expect(cl.GetType()).To(Equal(clusterv3.Cluster_STATIC))
		Expect(cl.GetConnectTimeout().AsDuration()).To(Equal(5 * time.Second))
	})

	It("names an EDS document by cluster_name", func() {
		_, name, err := decodeDocument(entryFor("eds"), doc(map[string]any{
			"clusterName": "cluster-a",
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(name).To(Equal("cluster-a"))
	})

	DescribeTable("names self-naming documents",
		func(id, name string) {
			_, got, err := decodeDocument(entryFor(id), doc(map[string]any{"name": name}))
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(name))
		},
		Entry("LDS", "lds", "listener-a"),
		Entry("RDS", "rds", "route-a"),
		Entry("SDS", "sds", "secret-a"),
	)

	It("decodes an LDS document with an @type HCM typed_config", func() {
		res, name, err := decodeDocument(entryFor("lds"), doc(map[string]any{
			"name": "listener-a",
			"address": map[string]any{
				"socketAddress": map[string]any{"address": "0.0.0.0", "portValue": 8080},
			},
			"filterChains": []any{map[string]any{
				"filters": []any{map[string]any{
					"name": "envoy.filters.network.http_connection_manager",
					"typedConfig": map[string]any{
						"@type":      "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
						"statPrefix": "ingress_http",
						"routeConfig": map[string]any{
							"name": "local_route",
						},
						"httpFilters": []any{map[string]any{
							"name": "envoy.filters.http.router",
							"typedConfig": map[string]any{
								"@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router",
							},
						}},
					},
				}},
			}},
		}))
		Expect(err).NotTo(HaveOccurred())
		Expect(name).To(Equal("listener-a"))

		l, ok := res.(*listenerv3.Listener)
		Expect(ok).To(BeTrue())
		Expect(l.GetFilterChains()).To(HaveLen(1))
		Expect(l.GetFilterChains()[0].GetFilters()).To(HaveLen(1))
		Expect(l.GetFilterChains()[0].GetFilters()[0].GetTypedConfig()).NotTo(BeNil())
	})

	It("rejects an unknown field", func() {
		_, _, err := decodeDocument(entryFor("cds"), doc(map[string]any{
			"name":       "cluster-a",
			"bogusField": true,
		}))
		Expect(err).To(HaveOccurred())
	})

	It("rejects an invalid enum value", func() {
		_, _, err := decodeDocument(entryFor("cds"), doc(map[string]any{
			"name": "cluster-a",
			"type": "NOT_A_REAL_TYPE",
		}))
		Expect(err).To(HaveOccurred())
	})

	It("rejects a resource without a name", func() {
		_, _, err := decodeDocument(entryFor("cds"), doc(map[string]any{
			"type": "STATIC",
		}))
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("classify", func() {
	cluster := func(name, typ string) *unstructured.Unstructured {
		return doc(map[string]any{"name": name, "type": typ})
	}

	var cds typeEntry
	BeforeEach(func() { cds = entryFor("cds") })

	It("upserts an inserted resource", func() {
		z := zset.New()
		z.Insert(cluster("c1", "STATIC"), 1)

		ops, kerrs := classify(cds, z)
		Expect(kerrs).To(BeEmpty())
		Expect(ops.upserts).To(HaveKey("c1"))
		Expect(ops.deletes).To(BeEmpty())
	})

	It("deletes a net-negative resource", func() {
		z := zset.New()
		z.Insert(cluster("c1", "STATIC"), -1)

		ops, kerrs := classify(cds, z)
		Expect(kerrs).To(BeEmpty())
		Expect(ops.upserts).To(BeEmpty())
		Expect(ops.deletes).To(ConsistOf("c1"))
	})

	It("nets an update (-old, +new) into a single upsert", func() {
		z := zset.New()
		z.Insert(cluster("c1", "STATIC"), -1)
		z.Insert(cluster("c1", "EDS"), 1)

		ops, kerrs := classify(cds, z)
		Expect(kerrs).To(BeEmpty())
		Expect(ops.deletes).To(BeEmpty())
		Expect(ops.upserts).To(HaveKey("c1"))
		Expect(ops.upserts["c1"].(*clusterv3.Cluster).GetType()).To(Equal(clusterv3.Cluster_EDS))
	})

	It("treats equal opposite weights as a no-op", func() {
		z := zset.New()
		z.Insert(cluster("c1", "STATIC"), 1)
		z.Insert(cluster("c1", "STATIC"), -1)

		ops, kerrs := classify(cds, z)
		Expect(kerrs).To(BeEmpty())
		Expect(ops.upserts).To(BeEmpty())
		Expect(ops.deletes).To(BeEmpty())
	})

	It("reports a decode error", func() {
		z := zset.New()
		z.Insert(doc(map[string]any{"name": "c1", "bogusField": true}), 1)

		_, kerrs := classify(cds, z)
		Expect(kerrs).NotTo(BeEmpty())
	})

	It("rejects two asserted contents for one name", func() {
		z := zset.New()
		z.Insert(cluster("c1", "STATIC"), 1)
		z.Insert(cluster("c1", "EDS"), 1)

		ops, kerrs := classify(cds, z)
		Expect(kerrs).To(HaveLen(1))
		Expect(ops.upserts).To(BeEmpty())
		Expect(ops.deletes).To(BeEmpty())
	})
})
