# Benchmarks

Benchmark suite for the scalable-and-robust desired-state control paper.
One declarative controller definition, the architectures obtained purely
by compile-time transforms - state of the world, SotW + diff shipping,
open-loop incremental, incremental desired-state reconciliation - 
measured at exact protocol boundaries. E1-E3 exercise a microscopic
endpoints controller against an in-process plant (everything the
artificial setup can answer: scaling exponents, the drift taxonomy, the
actuation cost of a delayed watch); E5 is the macrobenchmark: the real
delta-gateway operator, stock envoy-gateway and stock istiod (its
Gateway API path is krt-based - hand-rolled incrementality vs. our
principled DBSP incrementality), each in its own process against a
shared envtest plant (real kube-apiserver + etcd), one driver writing
the load, watching statuses and tapping every xDS server with our
delta-ADS clients; E6 pits the Δ-policy audit controller against stock
Gatekeeper on the same kind of plant. (The former macro-convergence and
macro-robustness experiments were folded into E5 - their corners,
embedded-stack ablation and status-tampering methodology live on as E5
phases; the E3 slot now holds the synthetic dead-time study.)

| dir | question | headline metric |
|---|---|---|
| `E1-scaling/` | does reaction cost scale with the change or the world? | per-delta latency, update bytes vs. base size N |
| `E2-robustness/` | who survives plant tampering? | drift ‖plant - desired‖ over time, MTTR |
| `E3-deadtime/` | what does a delayed watch cost, and when does it oscillate? | actuation [entries/round] vs. the dead-time model K; recovery under write loss vs. K; stability on additive vs. idempotent plants |
| `E5-macro/` | the real controllers on a real API server: reconciler / open / sotw / smith vs. envoy-gateway vs. istiod (krt) | write -> xDS delivery / status landed vs. scale; cold start; drift + MTTR under status tampering; corners; API-path price |
| `E6-policy-audit/` | continuous vs. periodic policy audit (Δ-policy vs. stock Gatekeeper) | detection latency, transient-violation completeness, idle cost |

Shared machinery lives in `lib/`: controller pipelines (`endpoints.js`),
fixtures, the plant with per-mode apply semantics (`plant.js`), noise
generators, the trace generator (`traces.js`), the driver stacks - 
`k8sstack.js` (the unified out-of-process driver: kubeconfig writes,
status taps, delta-ADS xDS taps for both delta-gateway and
envoy-gateway), `dgwstack.js` (the embedded in-process stack, the
ablation baseline) and `polstack.js` (E6) - and the measurement
apparatus (`measure.js`). The shared envtest plant launcher (with the
apiserver request-audit log and the optional stock-gatekeeper spawn)
lives in `plant/`; the envoy-gateway serve wrapper in
`eg-harness/`. Each experiment directory has its own README with the
exact commands and parameters.

## Results (fast profile, laptop; re-measured on the shared plant - paper run pending)

The bullets below are the embedded-plant findings that motivated the
harness; the E5 sweep re-measures all of them with the apiserver + etcd
round trip in the loop for every contender.

- **Cost tracks the change, not the world.** Delta-gateway convergence
  is flat in world size: region-add lands in the data plane in ~16 ms
  median from R=5 to R=73 trace-shaped regions. The same pipeline
  compiled as SotW grows linearly - 5.1 s at R=73, a 327× gap - and
  envoy-gateway grows the same way (6.3 -> 79.9 ms for N=10 -> 300,
  crossing our flat line at N≈50).
- **Sustained churn is where SotW breaks.** At 50 endpoint events/sec
  over 20 gateways, delta-gateway holds 3.5 ms median with zero loss;
  envoy-gateway saturates between 2 and 5 events/sec, degrading to
  tens of seconds of latency with event loss (E5 saturation corner).
- **Closing the loop is free, and it is the only thing that survives
  tampering.** Reconciled mode matches open-loop latency at steady
  state, yet holds zero drift under continuous plant (E2) and status
  (E5 disturbance phase) tampering with MTTR ≤ 1 round; open-loop
  incremental drifts permanently, by construction.
- **Honest limits.** When the touched artifact is itself O(N) - one
  vhost with K=1000 routes, one load assignment with E=500 endpoints - 
  incremental convergence degrades to the artifact size and
  envoy-gateway's debounced rebuild catches up (E5 corners).

## Running

Build the runtime once, then drive everything through the Makefile:

```sh
make prep                         # once per machine: dbsp + envtest binaries +
                                  # all contenders (EG, istiod, gatekeeper) + plant
make all                          # E1, E2, E3, E5, E6, fast profile (laptop, ~hours)
make full                         # paper-sized grids (server; long)
make e5                           # one experiment
make e5 EG=no ISTIO=no            # E5 without the external contenders
make render                       # verification: driver smokes, then redo
                                  # all charts from existing CSVs
```

Each `run.sh` sweeps its grid (fresh `dbsp` process per point), writes
CSVs under `results/`, and renders `results.org` - description,
expectations, readings, pgfplots charts - to PDF/PNG via `render.sh`.
`SCALE=full` selects the paper-sized grid, `RENDER=no` skips rendering.
**Re-running an experiment deletes its `results/*.csv` first**; use
`make render` to refresh charts only. One-time per machine: `make prep`
provisions everything (dbsp, the envtest control-plane binaries, the
envoy-gateway clone + serve binary, the istiod and Gatekeeper binaries,
the plant launcher); the individual targets (`make envtest eg istio
gk`) remain for partial refreshes, and `EG=no` / `ISTIO=no` skip those
comparison points.

## License

Copyright 2026 by its authors. See [AUTHORS](/AUTHORS).

MIT License. See [LICENSE](/LICENSE).
