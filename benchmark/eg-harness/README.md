# envoy-gateway serve wrapper (overlay)

The envoy-gateway contender of the E5 macro benchmark. This directory is
an *overlay* on the upstream envoy-gateway source tree: the wrapper must
compile inside the envoy-gateway module because it boots the controller
in-process from `internal/` packages (config, provider, gatewayapi and
xds runners, certgen) - none of which are importable from an external
module. `make eg` (in the parent directory) clones upstream at the
pinned commit into `../.cache/envoy-gateway` and copies this directory
in as `benchmarks/delta/`; no fork is maintained.

Contents:

- `serve_test.go` - `TestServe`: hosts stock envoy-gateway as a pure
  controller process against the externally provisioned plant
  (`EG_KUBECONFIG`, the shared envtest plant with the Gateway API +
  Envoy Gateway CRDs installed), publishes
  `$EG_SERVE_DIR/handshake.json` (xDS address, delta-ADS client certs,
  gateway class), and idles until `$EG_SERVE_DIR/stop` appears. It
  measures nothing itself: the E5 driver writes the load, watches
  statuses and times deliveries at its own delta-ADS clients - the same
  boundaries as every other contender.
- `testdata/crds/` - pre-generated CRDs the plant installs: helm
  template guards stripped, `*.x-k8s.io` CRDs dropped (their CEL
  `format` library needs a newer apiserver than envtest 1.31).

The boot runs the provider, gateway-api and xDS runners exactly as the
stock binary wires them; it excludes the infra runner (envtest has no
kubelet to run provisioned proxies - conservative in envoy-gateway's
favor) and generates the xDS TLS material locally (the stock binary
reads it from the hardcoded `/certs` mount, which is why the wrapper
exists at all).

## Usage

E5's `run.sh` provisions, compiles and spawns this automatically. By
hand:

```sh
cd .. && make envtest eg     # once per machine: k8s binaries + pinned EG clone
cd .cache/envoy-gateway
GOWORK=off go test -c -tags benchmark -o benchmarks/delta/delta.test ./benchmarks/delta
../plant/plant -crd-dir ../eg-harness/testdata/crds -gk=false &   # the plant
(cd benchmarks/delta && EG_SERVE_DIR=/tmp/eg-serve EG_KUBECONFIG=/tmp/plant/kubeconfig \
  ./delta.test -test.run TestServe -test.timeout 4h)
```

A known envoy-gateway quirk the E5 changesets encode (a candidate for
the paper's discussion): a route added to a pre-existing *empty*
gateway does not change the published snapshot, so L4 deltas are timed
as gateway+route unit pairs.
