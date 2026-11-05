#!/usr/bin/env bash
# E3 sweep: actuation under feedback dead time, the window ladder in the
# clean plant.
#
# The plant runs at a fixed dead time DT rounds; the dual-rate Smith is
# swept over its window K (ticks, one per round) from the plain Reconciler
# (K = 0, no window) up past the first window that absorbs the dead time:
# K >= DT actuates every correction exactly once, K < DT releases
# corrections before their echo and re-emits them.
#
#   ./run.sh            fast profile
#   SCALE=full ./run.sh longer traces, denser K grid
#   RENDER=no ./run.sh  skip chart rendering

set -eu
cd "$(dirname "$0")"

DBSP="${DBSP:-../../js/bin/dbsp}"
[ -x "$DBSP" ] || { echo "dbsp binary not found at $DBSP - run 'make build' first (or set DBSP)" >&2; exit 1; }
SCALE="${SCALE:-fast}"
RENDER="${RENDER:-yes}"

rm -f results/*.csv
mkdir -p results

DT="${DT:-4}"
if [ "$SCALE" = full ]; then
    N="${N:-100}" ROUNDS="${ROUNDS:-400}"
    KS="${KS:-1 2 3 4 5 6}"
else
    N="${N:-30}" ROUNDS="${ROUNDS:-150}"
    KS="${KS:-1 2 4 6}"
fi

run() {
    echo ">>> $*"
    "$DBSP" bench.js "$@" 2>&1 | grep -iE "E3 |STATS |error" || true
}

# --- Amplification: actuation volume vs. the window. The Reconciler is the
# K = 0 rung; the dual-rate Smith is swept over K past the dead time. ---
run --exp=amp --mode=reconciled --dt="$DT" --n="$N" --rounds="$ROUNDS"
for k in $KS; do
    run --exp=amp --mode=smith --dt="$DT" --k="$k" --n="$N" --rounds="$ROUNDS"
done

# --- Recovery under write loss: every correction entry is dropped with
# probability DROP (actuated but lost). The Reconciler re-emits U on the
# next step; the smith loop re-asserts only when the window releases the
# correction unconfirmed, so recovery latency grows with K - the price of
# the silence the amplification sweep buys. ---
DROP="${DROP:-0.1}"
ROUNDS_REC=$((ROUNDS / 2))
run --exp=rec --mode=reconciled --dt="$DT" --drop="$DROP" --n="$N" --rounds="$ROUNDS_REC"
for k in $KS; do
    run --exp=rec --mode=smith --dt="$DT" --k="$k" --drop="$DROP" --n="$N" --rounds="$ROUNDS_REC"
done

# --- Oscillation: one disturbance at dead time dt=2 against three
# (plant, loop) configurations, the smith window matched to the dead time.
# The additive plant + Reconciler is unstable; the trace is cut at
# ROUNDS_OSC rounds while it is still legible (it diverges thereafter).
# (No additive+smith config: the compensated loop's internal dist models
# an idempotent plant, so a non-idempotent plant contradicts the loop's
# own model and the case is out of scope.) ---
ROUNDS_OSC=10
for cfg in "additive reconciled" "idempotent reconciled" "idempotent smith"; do
    set -- $cfg
    run --exp=osc --plant="$1" --mode="$2" --dt=2 --k=2 --n=8 --rounds="$ROUNDS_OSC"
done

# --- Disturbance rejection vs. the window: the same single disturbance at
# the full plant dead time DT, swept over K on the idempotent plant. A
# short window keeps re-correcting until its release catches the echo; a
# window of the dead time retires the correction once. ---
ROUNDS_REJ=$((3 * DT))
run --exp=osc --plant=idempotent --mode=reconciled --dt="$DT" --n=8 --rounds="$ROUNDS_REJ"
for k in $KS; do
    run --exp=osc --plant=idempotent --mode=smith --dt="$DT" --k="$k" --n=8 --rounds="$ROUNDS_REJ"
done

if [ "$RENDER" = yes ]; then
    ../render.sh results.org
fi
