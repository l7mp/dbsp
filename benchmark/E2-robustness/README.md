# E2 - robustness: who survives plant tampering?

The E1 endpoints controller under out-of-band plant disturbance
(delete / corrupt / insert at rate λ per round), three architectures:
`sotw` (level-triggered: the full recomputed state overwrites the plant
wholesale, healing next round), `incremental` (open loop,
disturbances are permanent), `reconciled` (desired-state feedback, heals
in one step). Metrics: steady-state drift ‖plant - desired‖ and MTTR
after a disturbance burst.

Each round: churn one pod IP (background workload), let the controller
react, inject disturbances, sample drift.

## Run

```sh
./run.sh                 # fast profile (N=100, 300 rounds)
SCALE=full ./run.sh      # paper profile (N=300, 1000 rounds)
RENDER=no ./run.sh       # skip chart rendering
```

Single point:

```sh
../../../../js/bin/dbsp bench.js --mode=reconciled --noise=0.5
```

## Parameters (bench.js)

| flag | default | meaning |
|---|---|---|
| `--mode` | (required) | sotw, incremental, reconciled |
| `--n` | 100 | world size |
| `--rounds` | 300 | trace length |
| `--noise` | 0.2 | disturbance rate λ per round |
| `--burst` | 0 | if > 0: inject B disturbances at round T/2, measure MTTR |
| `--seed` | 42 | PRNG seed |
| `--timeout-ms` | 60000 | per-run timeout |

## Outputs

- `results/E2-steady-<mode>.csv` - one row per λ: mean/max drift, cumulative noise.
- `results/E2-burst-summary.csv` - one row per mode: burst round, healed round, MTTR.
- `results/E2-trace-<mode>-<l{rate}|burst>.csv` - per-round drift timeseries.
- `results.org` -> `results.pdf` + `results-*.png` via `../render.sh`.
