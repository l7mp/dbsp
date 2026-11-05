#!/usr/bin/env bash
# E2 sweep: drift under plant disturbance.
#
#   ./run.sh            fast profile
#   SCALE=full ./run.sh longer traces, more rates
#   RENDER=no ./run.sh  skip chart rendering

set -eu
cd "$(dirname "$0")"

DBSP="${DBSP:-../../js/bin/dbsp}"
[ -x "$DBSP" ] || { echo "dbsp binary not found at $DBSP - run 'make build' first (or set DBSP)" >&2; exit 1; }
SCALE="${SCALE:-fast}"
RENDER="${RENDER:-yes}"

rm -f results/*.csv results/heal/*.csv
mkdir -p results

# The churn period follows the noise rate, P = 10 / lambda: about ten
# disturbances land between two churns, so M1's sawtooth climbs about ten
# objects before each rewrite. N is large against lambda * ROUNDS so that
# the open loops do not saturate.
if [ "$SCALE" = full ]; then
    N=2000 ROUNDS=1000 BURST_ROUNDS=200 BURST=50 HEAL_N=10000
    RATES="0.05 0.1 0.2 0.5"
else
    N=300 ROUNDS=300 BURST_ROUNDS=100 BURST=20 HEAL_N=1000
    RATES="0.1 0.5"
fi
MODES="sotw sotw-diff incremental reconciled"

run() {
    echo ">>> $*"
    "$DBSP" bench.js "$@" 2>&1 | grep -iE "E2 |STATS |error" || true
}

for mode in $MODES; do
    for rate in $RATES; do
        period=$(awk -v l="$rate" 'BEGIN { printf "%d", 10 / l + 0.5 }')
        run --mode="$mode" --n="$N" --rounds="$ROUNDS" --noise="$rate" --churn-period="$period"
    done
done

# The healing time of the closed loop at a large world, for the text: the
# time from a disturbance to its correction (results/heal/E2-heal.csv; its
# own directory, so its trace does not replace the sweep's).
run --mode=reconciled --n="$HEAL_N" --rounds=200 --noise=0.5 --churn-period=20 --out=results/heal

# Recovery from a disturbance burst (MTTR), kept out of the paper.
for mode in $MODES; do
    run --mode="$mode" --n="$N" --rounds="$BURST_ROUNDS" --burst="$BURST"
done

if [ "$RENDER" = yes ]; then
    ../render.sh results.org
fi
