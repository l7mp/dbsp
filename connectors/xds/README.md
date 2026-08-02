# `connectors/xds`

An xDS sink that turns DBSP output topics into an Envoy control plane serving typed xDS resources
over gRPC.

Each xDS type (LDS, CDS, RDS, EDS, SDS, with ECDS/SRDS/VHDS/RTDS dropping out of the same
machinery) is its own materialized view. Bind one output topic per type and one `Consumer` per
topic, all sharing one `Server` (one gRPC endpoint, one cache). The Server is ADS-capable, so the
connector can drive a minimal Envoy gateway declaratively: listeners -> routes -> clusters ->
endpoints as four pipeline outputs.

## Usage

```go
srv, _ := xds.NewServer(xds.ServerConfig{Address: ":18000", Runtime: rt})
rt.Add(srv)
rt.Add(srv.Consumer(xds.ConsumerConfig{Name: "cds", OutputName: "clusters",  Type: "cds"}))
rt.Add(srv.Consumer(xds.ConsumerConfig{Name: "eds", OutputName: "endpoints", Type: "eds"}))
rt.Add(srv.Consumer(xds.ConsumerConfig{Name: "lds", OutputName: "listeners", Type: "lds"}))
rt.Add(srv.Consumer(xds.ConsumerConfig{Name: "rds", OutputName: "routes",    Type: "rds"}))
```

## Status

Bidirectional and tested, ADS only. Egress (serve Envoys): `Server` (one `LinearCache` per type behind
a `MuxCache`, serving SotW + Delta ADS), plus two consumers: `Updater` (delta, `UpdateResources`) and
`Setter` (snapshot, `SetResources`, delete-by-omission). Ingest (consume an upstream control plane):
`Lister` (SotW client) and `Watcher` (hand-rolled Delta client, last-seen `-old,+new`). Codec in
`resource.go` (document<->proto via protojson). An in-process ADS test client (`testclient.go`) plus a
full ingest loop against our own `Server` cover it without a real Envoy.

Exposed to JS as a flat `xds` global: `xds.server.start({name?, address})`, `xds.update`/`xds.set`
(bind egress consumers to a server by name), `xds.watch` (ingest an upstream: delta by default,
`level: true` for SotW snapshots). The ingest
producers speak to real management servers on their own terms: `node`/`nodeCluster`/`nodeMetadata`
carry the workload identity config scoping keys off (envoy-gateway matches `node.cluster`, istiod
reads the metadata's `NAMESPACE`/`CLUSTER_ID`/`LABELS`), `tls` enables mTLS client material, and
`resources` subscribes by explicit resource name (Endpoint and Route resources are non-wildcard by
xDS spec). `wellknown.go` registers the extension protos real gateway configurations embed as `Any`;
an unregistered type surfaces as a loud per-resource error naming the type URL.
