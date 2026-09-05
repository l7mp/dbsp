#!/usr/bin/env bash
# E1 sweep: per-delta latency and update size vs. base-config size.
#
#   ./run.sh            fast profile (laptop-sized, minutes)
#   SCALE=full ./run.sh full profile (paper-sized, hours; run on a server)
#   RENDER=no ./run.sh  skip chart rendering
#
# Every (case, mode, N) point runs in a fresh dbsp process; bench.js
# appends one summary row per point to results/E1-<case>-<mode>.csv.

set -eu
cd "$(dirname "$0")"

DBSP="${DBSP:-../../js/bin/dbsp}"
[ -x "$DBSP" ] || { echo "dbsp binary not found at $DBSP - run 'make build' first (or set DBSP)" >&2; exit 1; }
SCALE="${SCALE:-fast}"
RENDER="${RENDER:-yes}"

rm -f results/*.csv
mkdir -p results

# point <case> <mode> <n> <reps> [--indexed]
point() {
    local kase="$1" mode="$2" n="$3" reps="$4"; shift 4
    echo ">>> case=$kase mode=$mode n=$n reps=$reps $*"
    "$DBSP" bench.js --case="$kase" --mode="$mode" --n="$n" --reps="$reps" "$@" 2>&1 |
        grep -iE "E1 case=|STATS |error" || true
}

if [ "$SCALE" = full ]; then
    NS_FAST="${NS_FAST:-10 20 30 50 100 200 300 500 1000 2000 3000 5000 10000 20000 30000 50000 100000}"
    NS_SLOW="${NS_SLOW:-10 20 30 50 100 200 300 500 1000 2000}"
    NS_MID="${NS_MID:-10 20 30 50 100 200 300 500 1000 2000 3000 5000 10000}"
    REPS="${REPS:-20}" REPS_SLOW="${REPS_SLOW:-5}"
else
    NS_FAST="${NS_FAST:-10 30 100 300 1000 3000 10000}"
    NS_SLOW="${NS_SLOW:-10 30 100 300}"
    NS_MID="${NS_MID:-10 30 100 300 1000 3000}"
    REPS="${REPS:-10}" REPS_SLOW="${REPS_SLOW:-3}"
fi

# join: 1 service, N pods, no aggregation. The headline case.
for n in $NS_FAST; do point join incremental "$n" "$REPS"; done
for n in $NS_FAST; do point join reconciled  "$n" "$REPS"; done
# The state-of-the-world contenders recompute over full state, so they
# get few reps; the join recompute is linear, so they can afford the
# mid list (the quadratic pair case below stays on the short one). sotw
# is Model 1 (input adapters only: the level ships); sotw-diff is Model
# 2 (the plain commit: the output adapter ships the delta of the same
# recompute).
for n in $NS_MID; do point join sotw        "$n" "$REPS_SLOW"; done
for n in $NS_MID; do point join sotw-diff   "$n" "$REPS_SLOW"; done

# pair: N services 1:1 N pods, @groupBy. Cartesian join: the SotW side is
# quadratic, so it gets the short list.
for n in $NS_MID;  do point pair incremental "$n" "$REPS"; done
for n in $NS_MID;  do point pair reconciled  "$n" "$REPS"; done
for n in $NS_SLOW; do point pair sotw        "$n" "$REPS_SLOW"; done

# pair with the indexed equi-join: same shape, hash lookups instead of
# pair enumeration. The indexed SotW recompute is linear, so it affords
# the mid list like the join case.
for n in $NS_FAST; do point pair incremental "$n" "$REPS" --indexed; done
for n in $NS_MID;  do point pair sotw        "$n" "$REPS_SLOW" --indexed; done

# fanout: 1 service, N pods, one @groupBy group of size N. The output
# document is O(N): the honest lower bound for every mode. The SotW
# recompute is linear here too, so both lines share the mid list.
for n in $NS_MID; do point fanout incremental "$n" "$REPS"; done
for n in $NS_MID; do point fanout sotw        "$n" "$REPS_SLOW"; done

if [ "$RENDER" = yes ]; then
    ../render.sh results.org
fi
