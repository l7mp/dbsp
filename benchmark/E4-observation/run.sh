#!/usr/bin/env bash
# E4 sweep: observation is not correction.
#
#   ./run.sh            fast profile
#   SCALE=full ./run.sh paper profile (run on a server)
#
# Every (mode, N) point runs in a fresh dbsp process and appends one row to
# results/E4-<mode>.csv; raw-E4.csv holds one row per disturbance.

set -eu
cd "$(dirname "$0")"

DBSP="${DBSP:-../../js/bin/dbsp}"
[ -x "$DBSP" ] || { echo "dbsp binary not found at $DBSP - run 'make build' first (or set DBSP)" >&2; exit 1; }
SCALE="${SCALE:-fast}"

rm -f results/*.csv
mkdir -p results

if [ "$SCALE" = full ]; then
    NS="100 300 1000 3000 10000 30000 100000"
    REPS=30 REPS_SLOW=10 SLOW_FROM=30000
else
    NS="100 300 1000 3000"
    REPS=12 REPS_SLOW=6 SLOW_FROM=3000
fi

run() {
    echo ">>> $*"
    "$DBSP" bench.js "$@" 2>&1 | grep -iE "E4 |STATS |error" || true
}

# The state-of-the-world modes take fewer disturbances in the largest
# worlds, where each one costs them seconds.
for mode in sotw sotw-diff incremental reconciled; do
    for n in $NS; do
        reps=$REPS
        if [ "$n" -ge "$SLOW_FROM" ] && { [ "$mode" = sotw ] || [ "$mode" = sotw-diff ]; }; then
            reps=$REPS_SLOW
        fi
        run --mode="$mode" --n="$n" --reps="$reps"
    done
done
