// Package xds turns DBSP output topics into an Envoy xDS control plane. Each
// xDS type (LDS, CDS, RDS, EDS, SDS, ...) is a materialized view: a Consumer
// marshals the documents of one output topic into the typed proto and writes
// them into a shared Server's cache, served over gRPC (ADS plus per-type
// streams).
//
// The pipeline projects free-form documents shaped exactly like the xDS
// resource (no metadata/spec envelope) because xDS resources are self-naming
// (Cluster.name, RouteConfiguration.name, Listener.name,
// ClusterLoadAssignment.cluster_name). The connector is a dumb sink: marshal
// the document, unmarshal into the typed proto, apply.
package xds

import (
	"sort"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	resource "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/proto"
)

// typeEntry describes one xDS resource type: its xDS type URL and a factory
// that returns a fresh, empty proto message of that type.
type typeEntry struct {
	url string
	new func() proto.Message
}

// registry maps a short, stable type id (the value carried in ConsumerConfig.Type
// and in a declarative target's Kind slot) to its xDS type entry. The id is the
// only place a bare xDS document gets its type attached, because the document
// itself is not self-describing. New types (ecds/srds/vhds/rtds) slot in here.
var registry = map[string]typeEntry{
	"lds": {resource.ListenerType, func() proto.Message { return &listenerv3.Listener{} }},
	"cds": {resource.ClusterType, func() proto.Message { return &clusterv3.Cluster{} }},
	"rds": {resource.RouteType, func() proto.Message { return &routev3.RouteConfiguration{} }},
	"eds": {resource.EndpointType, func() proto.Message { return &endpointv3.ClusterLoadAssignment{} }},
	"sds": {resource.SecretType, func() proto.Message { return &tlsv3.Secret{} }},
}

// lookupType resolves a type id to its registry entry, reporting whether the id
// is known. Callers validate at construction (fail-fast on unknown).
func lookupType(id string) (typeEntry, bool) {
	e, ok := registry[id]
	return e, ok
}

// KnownTypes returns the registered type ids in a stable, sorted order, for
// fail-fast validation messages.
func KnownTypes() []string {
	ids := make([]string, 0, len(registry))
	for id := range registry {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
