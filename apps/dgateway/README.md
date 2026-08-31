# Δ-gateway: A declarative Kubernetes Gateway API controller for Envoy

Δ-gateway is a [Gateway API](https://gateway-api.sigs.k8s.io/) controller written in the
declarative DBSP style: a small JavaScript loader plus incremental aggregation pipelines. The
controller reconciles Gateway API resources (and the Services, EndpointSlices, Secrets, Namespaces
and BackendTLSPolicies they reference) to two kinds of outputs, kept incrementally up to date:

- **Gateway API statuses**: GatewayClass, Gateway with per-listener status, routes with per-parent
  status, written through a desired-state reconciler loop, and
- **Envoy xDS configuration** (LDS/RDS/CDS/EDS) served over delta ADS by the in-process xDS server.

The scope is L7 (HTTP and HTTPS) routing with TLS termination, SNI, the core HTTP filters and
upstream TLS via BackendTLSPolicy. Δ-gateway passes the [Gateway API conformance
suite](https://gateway-api.sigs.k8s.io/docs/concepts/conformance) for the features it declares.

See the [Δ-gateway user guide](../../doc/apps-dgw-overview.md) for the full documentation.

## Quick start

Δ-gateway does not create the Kubernetes resources to run the dataplane proxies so you have to
deploy your own Envoy pods manually and connect them to the xDS/ADS server exposed by the
controller and it will not create LoadBalancer Services either to expose your gateways; this is
just a declarative controller to demo DBSP.

1. Build the runtime once (from the workspace root).
   ```bash
   cd js && go build -o bin/dbsp ./cmd && cd ..
   ```
   
2. Run the self-contained test suite against a test xDS client (no Envoy)
   ```bash
   ./js/bin/dbsp apps/dgateway/index.js test
   ```
   
3. Install the Gateway API CRDs:
   ```bash
   kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/latest/download/standard-install.yaml
   ```

4. Start the controller against a real cluster with a pre-deployed Envoy. Note that Envoy must be
   manually connected to the controller. Note also that `--debug dumps` logs every event of every
   layer.
   
   ```bash
   ./js/bin/dbsp apps/dgateway/index.js controller --xds-address :18000 --debug dumps
   ```

5. Run the full Gateway API conformance suite, locally, rootless.
   
   ```bash
   apps/dgateway/conformance/run.sh
   ```

## License

Copyright 2026 by its authors. See [AUTHORS](/AUTHORS).

MIT License. See [LICENSE](/LICENSE).
