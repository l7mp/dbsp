# E1 - scaling: does reaction cost scale with the change or the world?

One declarative endpoints controller, four architectures obtained purely
by commit boundary configuration and compile-time transforms - `sotw`
(Model 1: input adapters only, full recompute, full-state ship),
`sotw-diff` (Model 2: the plain commit, same recompute, delta ship),
`incremental` (open-loop O(|Δ|)), `reconciled` (incremental +
desired-state feedback) - measured against an in-process plant. Metric: per-delta latency and update size as a function of
base-config size N.

Cases: `ratio` (the headline: N pods, k per service, the indexed join and
@groupBy; the measured change is a pod IP churn, `--k=1|10|100`, the
default sweep), and the legacy cases (`CASES=legacy ./run.sh`): `join` (1
service ⋈ N pods), `pair` (N services ⋈ N pods via @groupBy; the SotW side
is quadratic unless `--indexed`), `fanout` (one O(N) output document).
`delta_writes` counts objects written per change, one per key.

## Run

```sh
./run.sh                 # fast profile (laptop, minutes)
SCALE=full ./run.sh      # paper profile (N up to 100k; hours, use a server)
RENDER=no ./run.sh       # skip chart rendering
```

Single point:

```sh
../../../../js/bin/dbsp bench.js --case=join --mode=incremental --n=1000
```

## Parameters (bench.js)

| flag | default | meaning |
|---|---|---|
| `--case` | pair | ratio, join, pair, fanout |
| `--k` | 1 | pods per service (ratio case) |
| `--mode` | incremental | sotw, sotw-diff, incremental, reconciled |
| `--n` | 100 | base-config size (pods/services) |
| `--reps` | 20 | measured repetitions per point |
| `--warmup` | 3 | warmup deltas before measuring |
| `--indexed` | off | indexed equi-join instead of pair enumeration (always on in the ratio case) |
| `--timeout-ms` | 120000 | per-run timeout |

## Outputs

- `results/E1-<case>-<mode>.csv` (`E1-ratio<k>-<mode>.csv` for the ratio
  case) - one summary row per (case, mode, N): median/p95/mean/min/max
  latency, delta entries/bytes, objects written, write bytes.
- `results/raw-E1.csv` - per-repetition latencies.
- `results.org` -> `results.pdf` + `results-*.png` via `../render.sh`.
