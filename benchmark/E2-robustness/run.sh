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

rm -f results/*.csv
mkdir -p results

if [ "$SCALE" = full ]; then
    N=300 ROUNDS=1000 BURST_ROUNDS=200 BURST=50
    RATES="0.05 0.1 0.2 0.5 1"
else
    N=100 ROUNDS=300 BURST_ROUNDS=100 BURST=20
    RATES="0.05 0.1 0.2 0.5 1"
fi

run() {
    echo ">>> $*"
    "$DBSP" bench.js "$@" 2>&1 | grep -iE "E2 mode=|STATS |error" || true
}

# Steady-state drift at increasing noise rates. The 0.2 traces feed the
# timeseries figure; every rate contributes a point to the drift-vs-rate
# figure.
for mode in sotw incremental reconciled; do
    for rate in $RATES; do
        run --mode="$mode" --n="$N" --rounds="$ROUNDS" --noise="$rate"
    done
done

# Recovery from a disturbance burst (MTTR).
for mode in sotw incremental reconciled; do
    run --mode="$mode" --n="$N" --rounds="$BURST_ROUNDS" --burst="$BURST"
done

if [ "$RENDER" = yes ]; then
    ../render.sh results.org
fi
