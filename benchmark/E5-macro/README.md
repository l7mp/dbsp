# E5 - the macrobenchmark: every contender on the same plant

The full end-to-end comparison for the paper: delta-gateway compiled
into its four control-loop architectures (`reconciler` = incremental +
reconciler, `open` = incremental without the reconciler, `sotw` = full
recompute + delta ship via the snapshot jacket, `smith` = incremental +
the Smith dead-time compensator) against **stock envoy-gateway** (recompute on
change, ship deltas) and **stock istiod** (whose Gateway API
implementation is built on krt - Istio's hand-rolled incremental
controller runtime, the intuition-driven counterpart of our principled
DBSP incrementality), all running **out of process against a shared
envtest plant** (a real kube-apiserver + etcd, one fresh plant per grid
point). Every number crosses the same boundaries:

```
            writes (kubeconfig, sharded, 500 qps)          statuses (watch)
   driver -----------------> envtest plant <---------------- driver
                                   ^ v watch / status writes
                        contender (own process, own client)
                                   | xDS
   driver <-- delta-ADS client ----+   (plaintext for dgw, mTLS for EG)
```

Timing contract, identical for every contender: **t0** = driver issues
the mutation, **status** = the object's status lands back at the
driver's watch, **xds** = the delta arrives at the driver's delta-ADS
client connected to the contender's xDS server (actual delivery over
the wire, mTLS included for envoy-gateway - not a self-reported
internal timestamp). The driver, the write path, the plant flags, the
clocks and the convergence predicates are byte-for-byte shared; the
only per-system differences are the xDS transport (envoy-gateway
requires mTLS) and snapshot scoping (envoy-gateway scopes per gateway,
so the driver attaches one client per watched gateway; delta-gateway
serves one wildcard client).

Contenders are spawned by `run.sh`: the delta-gateway controller via
`dbsp ../../apps/dgateway/index.js controller --mode=...`, envoy-gateway
via the serve wrapper (`../eg-harness/`, stock runners at the pinned
upstream commit) pointed at the plant with `EG_KUBECONFIG`, and istiod
as the stock `pilot-discovery` binary (`make istio`) with
`--kubeconfig`, alpha Gateway API kinds on and manual-deployment
gateways (the driver provisions each gateway's workload and its clients
impersonate the gateway proxies - see `../lib/k8sstack.js`).
A `/proc` sampler and the apiserver request-audit log price each
contender with the same instruments (CPU, RSS, API load by user agent).

This experiment absorbed E3 and E4 (2026-07-16): the corner cases and
the embedded-stack ablation come from E3, the status-tampering
disturbance benchmark from E4. E1/E2 keep everything the artificial
in-process setup can answer; E5 is everything real.

## Workloads

Trace-shaped worlds from `lib/traces.js` (deterministic, seeded):

- `multiregion` - R regions × (1 Gateway + 1 HTTPRoute + 1 Service),
  modeled on a production multi-region L4 trace; one shared
  GatewayClass; scales the GATEWAY dimension. Changesets: region-add /
  region-delete / endpoint-add / endpoint-delete / endpoint-update.
- `sock-shop` - R regions × the Sock Shop reference app (~13
  HTTPRoutes per gateway); scales the ROUTES-PER-GATEWAY dimension.
  Changesets: route-add / route-update / route-delete + the endpoint
  set.

Phases in `run.sh`:

1. **Convergence grid** (`bench.js`) - per-changeset write->xds and
   write->status latency plus cold-start preload, per trace ×
   architecture × scale.
2. **Disturbance rejection** (`disturb.js`, from E4) - out-of-band
   status tampering through the status subresource; drift vs. an oracle
   and burst-MTTR, per architecture (does envoy-gateway ever heal?).
3. **Corners** (`corner.js`, from E3) - mega-vhost (1 gateway × K
   routes), endpoint-heavy (E endpoints per backend), and saturation
   (endpoint churn at RATE events/s: the knee is the sustainable churn
   limit), both systems on the same plant.
4. **Embedded ablation + fan-out + storm** (`ablation.js`) - the same
   controller hosted in-process on the embedded API server: the delta
   against phase 1 prices the real Kubernetes API path. The xDS
   fan-out (C clients) and reconnect-storm scenarios live here too
   (the API plant is not in their measured path). `probe.js` is the
   cold-start diagnostic on this stack.

## Run

```sh
make -C .. envtest eg    # once per machine: control-plane binaries + EG clone
./run.sh                 # fast profile (laptop)
SCALE=full ./run.sh      # paper profile (server)
EG=no ./run.sh           # skip the envoy-gateway points
RENDER=no ./run.sh       # skip chart rendering
```

Progress: `tail -f /tmp/e5.log`. Run at most ONE sweep at a time (fixed
xDS ports, one envtest per point); clean leftovers with
`pkill -9 -x etcd; pkill -9 -x kube-apiserver`.

Single points (plant + contender must be up; see `run.sh` `point()` for
the exact spawn commands):

```sh
../plant/plant -crd-dir ../eg-harness/testdata/crds -gk=false &
KUBECONFIG=/tmp/gw-plant/kubeconfig ../../js/bin/dbsp ../../apps/dgateway/index.js \
    controller --mode=reconciler --xds-address 127.0.0.1:19001 &
../../js/bin/dbsp bench.js --trace=multiregion --regions=18 --system=dgw \
    --mode=reconciler --xds-address=127.0.0.1:19001
```

## Trace notes

The `multiregion` trace models a production multi-region L4 gateway;
the tuples are carried as HTTP listener + HTTPRoute (the operator's
declared surface is HTTP-only) - the topology and the churn model are
the L4 gateway's, and at the control plane the two shapes cost the
same. `multiregion-l4` and `stunner-udp` are accepted as legacy
aliases.

## Parameters

`bench.js` (grid): `--trace`, `--regions`, `--system dgw|eg`,
`--mode reconciler|open|sotw|smith` (must match the running controller),
`--reps`, `--seed`, `--handshake`, `--eg-handshake`, `--xds-address`,
`--timeout-ms`, `--out`.

`disturb.js` adds `--rounds`, `--noise` (tamperings/round, Poisson),
`--burst` (B tamperings at round T/2, measures MTTR).

`congestion.js` (phase 2c): the SUT runs with a reduced `--qps` client
budget (real feedback dead time on a real apiserver); a burst of B
tamperings measures heal time for reconciler vs smith, and
`congestion_summary.py` joins each point with the PATCH counts from the
request log (SUT vs driver split by user agent).

`corner.js`: `--case mega-vhost|endpoint-heavy|saturation` with
`--routes K` / `--endpoints E` / `--rate`,`--duration`,`--gateways`.

`ablation.js`: the in-process flags (`--api-port`, `--xds-port`) plus
`--clients C` (fan-out) and `--storm` `--stagger-ms`.

## Outputs

- `results/E5-<trace>-<arch>-<changeset>.csv` - grid: xds/status
  median+p95, preload; `<arch>` ∈ reconciler/open/sotw/smith/eg/istio.
- `results/E5-preload-<arch>.csv` - cold start vs. scale.
- `results/E5-disturb-{steady,burst}.csv` + per-round traces - drift
  and MTTR per architecture.
- `results/corner-{megavhost,endpointheavy,saturation}-<arch>.csv`.
- `results/E5-ablation-*.csv` - embedded-stack grid (API-path price).
- `results/E5-fanout-endpoint-update.csv`, `results/E5-storm-*.csv`.
- `results/proc.csv` + `results/requests-<point>.log` - the price
  instruments.
- `results.org` -> `results.pdf` + `results-*.png` via `../render.sh`.
