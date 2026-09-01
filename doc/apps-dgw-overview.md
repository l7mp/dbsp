# Δ-gateway: a declarative Gateway API controller for Envoy

Δ-gateway is a [Gateway API](https://gateway-api.sigs.k8s.io/) controller written in the
declarative DBSP style: a small JavaScript loader plus aggregation pipelines, frozen into one
serialized runtime and loaded with `runtime.create`. It reconciles Gateway API resources (and the
Services, EndpointSlices, Secrets, Namespaces and BackendTLSPolicies they reference) to two kinds
of outputs, kept incrementally up to date:

- **Gateway API statuses**: GatewayClass, Gateway with per-listener status, routes with per-parent
  status, written through Patchers and, in reconciler mode, a desired-state control loop;
- **Envoy xDS configuration** (LDS/RDS/CDS/EDS) served over delta ADS by the in-process xDS
  server.

The scope is L7 (HTTP and HTTPS) routing with TLS termination, SNI, the core HTTP filters and
upstream TLS via BackendTLSPolicy. Δ-gateway passes the Gateway API conformance suite for the
features it declares. The code lives in `apps/dgateway/`.

## Architecture: three circuits around curated views

The operator is one DBSP runtime with three circuits meeting on internal streams:

```text
k8s sources -> [input] -> {GatewayClassView, GatewayView, RouteView, BackendView}
                          {GatewayClassStatusObserved, GatewayStatusObserved, ...}
views -------> [k8s]  -> {GatewayClassStatus, GatewayStatus, HTTPRouteStatus} -> Patchers
views -------> [xds]  -> {XdsListeners, XdsRouteConfigurations, XdsClusters, XdsEndpoints} -> xDS
```

The **input** circuit folds the watched Kubernetes objects into a small set of curated views, the
controller's internal state, one per major CRD. The **k8s** circuit renders the views into status
documents, and the **xds** circuit renders them into Envoy protojson resources. The views and
observed streams are internal wires: circuit outputs consumed by circuit inputs, bound to nothing,
yet observable live with `handle.subscribe()` since every stream is a topic named plainly by the
stream.

## Modes

The `--mode` flag selects the control loop; the programs are identical in every mode.

| Mode | What it is |
|---|---|
| `reconciler` (default) | The closed loop: status outputs emit the outstanding correction `U = ∫(δD − δY_U)`, re-emitted until the watch feedback confirms it, so tampering heals and lost writes retry by construction. |
| `open` | Open loop: statuses written once per change, no feedback. |
| `smith` | The dead-time compensated loop (`--smith-k`): every correction actuated exactly once, watch echoes retired by content. |
| `sotw` | Snapshot execution: the same programs with empty transform chains, compiled by the engine to ∫ -> Q -> D. Same connectors, same harness; exists as the recompute-everything baseline. |

## Running

```bash
cd js && go build -o bin/dbsp ./cmd && cd ..

# Self-contained test suite (no cluster, no Envoy), in either execution:
./js/bin/dbsp apps/dgateway/index.js test
./js/bin/dbsp apps/dgateway/index.js test --mode sotw

# Against a real cluster with a pre-deployed Envoy:
./js/bin/dbsp apps/dgateway/index.js controller --xds-address :18000 --debug dumps
```

Δ-gateway does not deploy dataplane proxies or LoadBalancer services: it is a declarative
controller demo. Envoy must be connected to the controller's xDS address manually. `--debug dumps`
attaches an observer to every layer (`pipeline.observe(layer, fn)` rides `handle.observe`).

