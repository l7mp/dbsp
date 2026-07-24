# The xDS connector

The xDS connector fills the same role as the Kubernetes connector, against a different target: it
serves typed Envoy configuration over gRPC instead of writing objects to an apiserver. A pipeline
that emits listeners, routes, clusters, and endpoints as four output topics is an Envoy control
plane.

The connector is bidirectional. On egress it runs an xDS server and publishes what the pipeline
computes to the Envoys connected to it. On ingest it acts as an xDS client against somebody else's
control plane, turning the configuration that server hands out into topics, which is how one
control plane's output becomes another pipeline's input.

Each xDS type is its own materialized view: bind one output topic per type, all sharing one server.
The connector is ADS-only, so all types travel over a single aggregated stream. Types are named by
their usual short forms, `lds` (listeners), `rds` (routes), `cds` (clusters), `eds` (endpoints), and
`sds` (secrets).

## Resource documents

xDS resources are self-naming: a `Cluster` carries its own `name`, a `ClusterLoadAssignment` its
`cluster_name`. There is no `metadata`/`spec` envelope, so a pipeline emits documents shaped exactly
like the resource itself, and the connector marshals each one straight into the typed proto.

```js
// One entry on a "cds" topic.
[[{ name: "backend", type: "EDS", edsClusterConfig: { edsConfig: { ads: {} } } }, 1]]
```

Extension protos that real configurations embed as `Any` values are registered by the connector; a
type it does not know surfaces as a loud per-resource error naming the type URL, rather than as
silently missing configuration.

## Egress: the server

Start a server, then bind one consumer per resource type to it:

```js
const server = xds.server.start({ address: "127.0.0.1:18000" });

xds.update("envoy.lds", { type: "lds" });
xds.update("envoy.rds", { type: "rds" });
xds.update("envoy.cds", { type: "cds" });
xds.update("envoy.eds", { type: "eds" });
```

`xds.server.start` returns the server's `name` and its `address`; with a port of `0` the address
reports the port actually bound, which is what tests and benchmarks use. Passing a `name` and then
naming it in the consumer options binds them explicitly, which is only needed when a script runs
more than one server.

The two egress consumers mirror the Kubernetes ones. **Updater** (`xds.update`) is the delta sink:
it applies the incoming Z-set as a change, adding and removing individual resources. **Setter**
(`xds.set`) is the snapshot sink: each event carries the complete set of resources of that type, and
anything absent from it is deleted. As on the Kubernetes side, the delta consumer is the one that
pairs with an incremental pipeline.

## Ingest: the client

The ingest producers connect to an upstream management server and turn what it serves into topics:

```js
xds.watch("upstream.cds", { type: "cds", address: "127.0.0.1:18000", node: "my-envoy" });
```

**Watcher** (`xds.watch`) speaks the delta protocol and emits changes, retracting the previous
version of a resource as it publishes the new one. **Lister** (`xds.list`) speaks the
state-of-the-world protocol and emits full snapshots.

A management server decides what to serve based on who is asking, so the identity options matter:
`node` and `nodeCluster` carry the workload identity that scopes the configuration, and
`nodeMetadata` carries the free-form metadata a server may key off. Envoy Gateway matches on the
node cluster, while istiod reads namespace, cluster, and label fields out of the metadata.

```js
xds.watch("upstream.eds", {
  type:      "eds",
  address:   "istiod.istio-system:15010",
  node:      "sidecar~10.0.0.1~pod.ns~ns.svc.cluster.local",
  resources: ["outbound|80||backend.ns.svc.cluster.local"],
});
```

Endpoint and route resources are non-wildcard by the xDS specification: a client must name what it
wants, which is what `resources` is for. Subscribing to them without it is not an error, it simply
returns nothing.

Where the upstream requires client certificates, `tls` supplies them:

```js
xds.watch("upstream.lds", {
  type:    "lds",
  address: "envoy-gateway:18000",
  tls:     { cert: "tls.crt", key: "tls.key", ca: "ca.crt", serverName: "envoy-gateway" },
});
```

The watcher survives upstream restarts: it re-syncs on reconnect and retracts whatever vanished
while it was away, so the topic keeps describing what the upstream actually serves.

## Which one to use

| Role | Delta | Snapshot |
|---|---|---|
| Egress, serve Envoys | `xds.update` | `xds.set` |
| Ingest, consume an upstream | `xds.watch` | `xds.list` |
