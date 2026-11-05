# E4 - observation: does seeing a disturbance correct it?

The endpoints controller (pair case, N services with one pod each, the
indexed join) in all four architectures, each watching the Endpoints it
writes: every out-of-band disturbance reaches every controller as a watch
event and triggers a step. The driver deletes, corrupts or inserts one
object at a time, waits until the circuit and the in-process plant are
quiet, and records per disturbance:

- whether the object is back to its desired content (an inserted object
  removed),
- the circuit steps the disturbance triggered (the runtime step counters,
  which also count the steps whose output is empty),
- the time the circuit spent stepping,
- the objects written.

The plant reports every change on the watch, the controller's own writes
included; an unchanged write produces no event. Expected: M1 repairs by
rewriting the whole level, and the repair's echo triggers a second full
recompute (2 steps, 2N writes, 2 cost(C)); M2 recomputes, finds no change
against its own last output and writes nothing (1 step, cost(C), never
corrected); M3 steps on the empty change in microseconds and writes
nothing (never corrected); M4 writes the one-object correction (2 steps,
1 write, independent of N).

## Run

```sh
./run.sh                 # fast profile
SCALE=full ./run.sh      # paper profile (N up to 10^5; use a server)
```

Single point:

```sh
../../js/bin/dbsp bench.js --mode=incremental --n=1000 --reps=30
```

## Parameters (bench.js)

| flag | default | meaning |
|---|---|---|
| `--mode` | reconciled | sotw, sotw-diff, incremental, reconciled |
| `--n` | 1000 | services (and pods) |
| `--reps` | 30 | disturbances (delete, corrupt, insert in turn) |
| `--quiet-ms` | 50 | quiet window, plus twice the longest step seen |
| `--seed` | 42 | victim selection |
| `--timeout-ms` | 120000 | per-disturbance timeout |

## Outputs

- `results/E4-<mode>.csv` - one row per N: fraction corrected, steps and
  objects written per disturbance, step time and time to the last step
  (median, p95).
- `results/raw-E4.csv` - one row per disturbance.
