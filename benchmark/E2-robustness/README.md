# E2 - robustness: who survives plant tampering?

The E1 endpoints controller (pair case, indexed join) under out-of-band
plant disturbance (delete / corrupt / insert at rate λ per round), four
architectures: `sotw` (M1: the full recomputed state overwrites the
plant wholesale, but only when an input changes), `sotw-diff` (M2: the
same recompute, its delta shipped; open loop), `incremental` (M3: open
loop), `reconciled` (M4: desired-state feedback, heals in one step). Only
M4 watches the Endpoints it writes. Metrics: drift (wrong objects) per
round, the closed loop's healing time, and MTTR after a disturbance
burst.

Each round: churn one pod IP if the round is a churn round (once every
`--churn-period` rounds, jittered by a quarter period), let the controller
react, inject the round's disturbances, sample drift. M1 drops to zero at
every churn and climbs in between (a sawtooth); M2 and M3 are identical
and accumulate; M4 stays at zero. Disturbance victims are drawn from the
managed objects, so every architecture sees the same sequence.

## Run

```sh
./run.sh                 # fast profile (N=300, 300 rounds)
SCALE=full ./run.sh      # paper profile (N=2000, 1000 rounds, churn period 10/λ)
RENDER=no ./run.sh       # skip chart rendering
```

Single point:

```sh
../../../../js/bin/dbsp bench.js --mode=reconciled --noise=0.5
```

## Parameters (bench.js)

| flag | default | meaning |
|---|---|---|
| `--mode` | reconciled | sotw, sotw-diff, incremental, reconciled |
| `--n` | 1000 | world size |
| `--rounds` | 1000 | trace length |
| `--noise` | 0.2 | disturbance rate λ per round |
| `--churn-period` | 50 | rounds between background churns (jittered) |
| `--burst` | 0 | if > 0: inject B disturbances at round T/2, measure MTTR |
| `--seed` | 42 | PRNG seed |
| `--timeout-ms` | 60000 | per-run timeout |

## Outputs

- `results/E2-steady-<mode>.csv` - one row per λ: mean/max drift, cumulative noise.
- `results/E2-heal.csv` - the closed loop's time from disturbance to correction (median, p95).
- `results/E2-burst-summary.csv` - one row per mode: burst round, healed round, MTTR.
- `results/E2-trace-<mode>-<l{rate}|burst>.csv` - per-round drift (wrong objects), cumulative noise, churn flag.
- `results.org` -> `results.pdf` + `results-*.png` via `../render.sh`.
