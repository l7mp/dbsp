# E3 - dead time: what does a delayed watch cost?

The E1/E2 endpoints controller under a *delayed* plant: the watch echo
that closes the loop lags the write by a feedback dead time of `k` steps.
The desired-state Reconciler re-emits the whole outstanding correction on
every step until the echo confirms it, so a dead time of `k` turns one
logical change into `O(k)` redundant applies. The SmithPredictor retires
the loop's own echoes by content against its delayed integral `z⁻¹U` and
emits each correction exactly once, with `k` appearing nowhere.

Three experiments:

- **amp** - actuation amplification. A fixed churn workload runs for R
  rounds at dead time `k`; we count the entries the plant applier receives.
  Swept over `k` for both loops. Reconciler grows linearly in `k`; Smith is
  flat.
- **rec** - recovery under write loss. Every correction entry is dropped
  with probability `--drop` (actuated but lost); each round measures the
  ticks until the churned state lands in the plant. The Reconciler
  re-emits on the next step; the smith loop re-asserts only after its
  K-step echo window drains, so recovery latency grows with K - the
  price of the silence `amp` buys.
- **osc** - the oscillation the theory warns about, and why Kubernetes
  never shows it. One disturbance at `k=2` against three (plant, loop)
  configurations. An *additive* plant (no idempotence) + Reconciler is
  unstable and rings; an *idempotent* plant (apiserver server-side apply is
  a `dist`) absorbs the re-emissions into waste with no oscillation; Smith
  corrects once.

Everything is synthetic and in-process - the dead time is injected by the
plant, so no apiserver, no envtest, no Kubernetes.

## Run

```sh
./run.sh                 # fast profile (N=30, 150 rounds, k up to 16)
SCALE=full ./run.sh      # paper profile (N=100, 400 rounds, k up to 32)
RENDER=no ./run.sh       # skip chart rendering
```

Single point:

```sh
../../js/bin/dbsp bench.js --exp=amp --mode=reconciled --k=8
../../js/bin/dbsp bench.js --exp=rec --mode=smith --k=5 --drop=0.1
../../js/bin/dbsp bench.js --exp=osc --plant=additive --mode=reconciled --k=2
```

## Parameters (bench.js)

| flag | default | meaning |
|---|---|---|
| `--exp` | amp | experiment: `amp` (amplification), `rec` (recovery) or `osc` (oscillation) |
| `--mode` | reconciled | circuit transform: `reconciled` or `smith` |
| `--plant` | = `--mode` | plant apply semantics: `idempotent` (set clamp), `additive` (no clamp) |
| `--k` | 4 | feedback dead time in steps (osc uses 2) |
| `--n` | 50 | world size (managed services) |
| `--rounds` | 200 | trace length |
| `--seed` | 42 | PRNG seed |

## Outputs

- `results/E3-amp-<mode>.csv` - actuation per round vs. dead time `k`.
- `results/E3-trace-<mode>-k<K>.csv` - per-round actuation trajectory.
- `results/E3-osc-<plant>-<mode>.csv` - the disturbed object's net weight
  per round.
- `results.org` / `results.pdf` / `results-*.png` - the report and charts.
