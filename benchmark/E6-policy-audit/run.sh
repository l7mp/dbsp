#!/usr/bin/env bash
# E6 policy-audit sweep: dpolicy vs stock Gatekeeper audit, one grid point
# (mode, N, P) per fresh envtest plant. The contenders run out of process
# (gatekeeper spawned by the harness, the dpolicy controller spawned here);
# the driver only writes load and watches statuses, and a /proc sampler
# records each contender's CPU/RSS with the same instrument.
set -eu
cd "$(dirname "$0")/.."

E6="${E6_DIR:-E6-policy-audit}"
DBSP=../js/bin/dbsp
HARNESS=plant/plant
GK_BIN="${GK_BIN:-.cache/gatekeeper/bin/gatekeeper}"
CRD_DIR=.cache/gatekeeper/charts/gatekeeper/crds
EXTRA_CRD_DIR=$E6/crds
SERVE_DIR=/tmp/gk-serve
PROGRESS=/tmp/e6-sweep.log

export KUBEBUILDER_ASSETS="${KUBEBUILDER_ASSETS:-$(ls -d "$HOME"/.local/share/kubebuilder-envtest/k8s/*-linux-amd64 | sort -V | tail -1)}"

MODES="${MODES:-dpolicy dpolicy-smith gk gk-cache}"
SCALE="${SCALE:-fast}"
# The audit interval is normalized to 10 s in both profiles: a periodic
# scanner's detection behavior is scale-free in L/interval (w3) and
# latency/interval (w1), so the compressed interval measures the same
# curves in a fraction of the wall clock. Gatekeeper's as-deployed default
# is 60 s  -  rerun with AUDIT_INTERVAL=60 LIFETIMES=15,30,60,90,180 for a
# confirmation point; results scale linearly.
if [ "$SCALE" = full ]; then
    NS="${NS:-1000 5000 10000}"
    W4_NS="${W4_NS:-2000 3500 7000}"
    PS="${PS:-1 10 50}"
    REPS="${REPS:-10}"
    AUDIT_INTERVAL="${AUDIT_INTERVAL:-10}"
    IDLE_SECS="${IDLE_SECS:-600}"
    LIFETIMES="${LIFETIMES:-2,5,8,15,30}"
    RATES="${RATES:-5}"
    EVENTS="${EVENTS:-100}"
    TIMEOUT="${TIMEOUT:-1800000}"
    SMITH_KS="${SMITH_KS:-1 2 3 4 5}"
    SMITH_K_DEFAULT="${SMITH_K_DEFAULT:-5}"
else
    NS="${NS:-200 1000}"
    W4_NS="${W4_NS:-500 3000}"
    PS="${PS:-1 10}"
    REPS="${REPS:-3}"
    AUDIT_INTERVAL="${AUDIT_INTERVAL:-10}"
    IDLE_SECS="${IDLE_SECS:-45}"
    LIFETIMES="${LIFETIMES:-2,5,15}"
    RATES="${RATES:-2}"
    EVENTS="${EVENTS:-20}"
    TIMEOUT="${TIMEOUT:-600000}"
    SMITH_KS="${SMITH_KS:-1 3}"
    SMITH_K_DEFAULT="${SMITH_K_DEFAULT:-3}"
fi
WORKLOADS="${WORKLOADS:-w1,w2,w3,w4,w5}"
# W2_PS: the P values whose grid points include the idle workload (the
# idle table is read at one policy); W4_PS: the P values of the extra w4
# density points. Both default to the whole grid.
W2_PS="${W2_PS:-$PS}"
W4_PS="${W4_PS:-$PS}"
# The congestion ladder (phase 1): w6 at one inventory size with LADDER_P
# required-labels constraints, so one flip is LADDER_P status corrections
# (plus the PSS corpus), dpolicy throttled to the realistic client budget
# (client-go's default 5 QPS,
# with an equal burst) so they queue behind it; the window runs from the
# reconciler (K = 0) up SMITH_KS. The Gatekeeper contenders ride along at
# their own default budget (the same 5 QPS).
LADDER_N="${LADDER_N:-1000}"
LADDER_P="${LADDER_P:-20}"
LADDER_QPS="${LADDER_QPS:-5}"
LADDER_BURST="${LADDER_BURST:-$LADDER_QPS}"
LADDER_REPS="${LADDER_REPS:-$REPS}"
# The batch bootstrap: dpolicy starts with --bootstrap manual (the loop
# open) and closes the loop by itself once its inputs have been quiet for
# SWITCH_QUIET ms; the driver waits the switch out before the workloads.
# BENCH_BOOTSTRAP=auto starts it with the loop closed from the first
# write: the cold-start points of phase 0, baseline only, at COLD_NS.
# The actuator dpolicy writes through: the plain Kubernetes connector (the
# paper's) unless SUT_PLAIN_K8S is empty, which leaves the production
# default (re-emissions composed and absorbed). dpolicy-smith is the
# dual-rate Smith at SMITH_K_DEFAULT seconds of window; SMITH_KS is the
# ladder of phase 1.
SUT_PLAIN_K8S="${SUT_PLAIN_K8S-yes}"
SWITCH_QUIET="${SWITCH_QUIET:-20000}"
COLD_NS="${COLD_NS:-$NS}"
# A cold start runs to this convergence timeout (ms); the first size that
# does not converge is the mode's wall and ends its line.
COLD_TIMEOUT="${COLD_TIMEOUT:-$TIMEOUT}"
BENCH_BOOTSTRAP="${BENCH_BOOTSTRAP:-manual}"
BENCH_VARIANT="${BENCH_VARIANT:-}"
PSS="${PSS:-yes}"
CONSTRAINT_KINDS="K8sRequiredLabels"
BENCH_PSS_FLAG=""
if [ "$PSS" = yes ]; then
    CONSTRAINT_KINDS="K8sRequiredLabels,K8sPSPPrivilegedContainer,K8sPSPHostFilesystem,K8sPSPCapabilities"
else
    BENCH_PSS_FLAG="--no-pss"
fi

note() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$PROGRESS"; }

mkdir -p $E6/results
rm -f $E6/results/e6.csv $E6/results/proc.csv
: > "$PROGRESS"

# Build what is missing (the gatekeeper baseline provisions itself).
[ -x "$HARNESS" ] || (cd plant && GOWORK=off go build -o plant .)
[ -x "$GK_BIN" ] || { note "provisioning gatekeeper (make gk)"; make -C . gk; }

cleanup() {
    touch $SERVE_DIR/stop 2>/dev/null || true
    [ -n "${SUT_PID:-}" ] && kill "$SUT_PID" 2>/dev/null || true
    [ -n "${SAMPLER_PID:-}" ] && kill "$SAMPLER_PID" 2>/dev/null || true
    [ -n "${HARNESS_PID:-}" ] && { sleep 2; kill "$HARNESS_PID" 2>/dev/null || true; }
    pkill -x gatekeeper 2>/dev/null || true
    sleep 1
    pkill -9 -x etcd 2>/dev/null || true
    pkill -9 -x kube-apiserver 2>/dev/null || true
    # A hard-killed control plane leaves its etcd data dir behind
    # (~130 MB per plant  -  a full sweep exhausts /tmp without this).
    rm -rf /tmp/k8s_test_framework_* 2>/dev/null || true
    SUT_PID=""; SAMPLER_PID=""; HARNESS_PID=""
}
trap cleanup EXIT

# sampler <label> <pid>: one CSV line per second with the contender's
# cumulative CPU ticks (utime+stime) and RSS pages from /proc.
sampler() {
    local label=$1 pid=$2
    while kill -0 "$pid" 2>/dev/null; do
        awk -v l="$label" -v p="$pid" -v t="$(date +%s%3N)" \
            '{print t "," l "," p "," $14+$15 "," $24}' "/proc/$pid/stat" \
            >> $E6/results/proc.csv 2>/dev/null || true
        sleep 1
    done
}

run_point() {
    local mode=$1 n=$2 p=$3 wl=${4:-$WORKLOADS}
    note "mode=$mode N=$n P=$p workloads=$wl: booting plant..."
    rm -rf $SERVE_DIR

    local harness_flags="-crd-dir $CRD_DIR -extra-crd-dir $EXTRA_CRD_DIR -audit-interval $AUDIT_INTERVAL"
    case $mode in
        dpolicy*) harness_flags="$harness_flags -gk=false" ;;
        gk)       harness_flags="$harness_flags -gk-bin $GK_BIN" ;;
        gk-cache) harness_flags="$harness_flags -gk-bin $GK_BIN -audit-from-cache" ;;
    esac
    PLANT_DIR=$SERVE_DIR ./$HARNESS $harness_flags > "/tmp/e6-harness-$mode-$n-$p.log" 2>&1 &
    HARNESS_PID=$!
    for i in $(seq 1 120); do [ -f $SERVE_DIR/handshake.json ] && break; sleep 1; done
    [ -f $SERVE_DIR/handshake.json ] || { note "harness failed, see /tmp/e6-harness-$mode-$n-$p.log"; return 1; }

    local bench_flags=""
    if [ "$mode" = dpolicy ] || [ "$mode" = dpolicy-smith ]; then
        local loop=reconciler k=""
        [ "$mode" = dpolicy-smith ] && { loop=smith; k=${SUT_SMITH_K:-$SMITH_K_DEFAULT}; }
        KUBECONFIG=$SERVE_DIR/kubeconfig $DBSP ../apps/dpolicy/index.js controller \
            --constraint-kinds "$CONSTRAINT_KINDS" --mode "$loop" \
            ${k:+--smith-window "$((k * 1000))"} \
            ${SUT_QPS:+--qps "$SUT_QPS"} ${SUT_BURST:+--burst "$SUT_BURST"} \
            --bootstrap "$BENCH_BOOTSTRAP" --switch-quiet-ms "$SWITCH_QUIET" ${SUT_PLAIN_K8S:+--plain-k8s-connector} \
            --no-violation-views > "/tmp/e6-$mode-$n-$p.log" 2>&1 &
        SUT_PID=$!
    else
        SUT_PID=$(grep -o '"gkPid": [0-9]*' $SERVE_DIR/handshake.json | grep -o '[0-9]*')
        [ "$mode" = gk-cache ] && bench_flags="--gk-cache"
    fi
    sampler "$mode-$n-$p" "$SUT_PID" &
    SAMPLER_PID=$!

    $DBSP $E6/bench.js --mode "$mode" --workloads "$wl" \
        --n "$n" --p "$p" --reps "$REPS" --idle-secs "$IDLE_SECS" \
        --lifetimes "$LIFETIMES" --rate "$RATES" --events "$EVENTS" \
        --interval "$AUDIT_INTERVAL" --switch-quiet-ms "$SWITCH_QUIET" \
        --bootstrap "$BENCH_BOOTSTRAP" ${BENCH_VARIANT:+--variant "$BENCH_VARIANT"} \
        --timeout "$TIMEOUT" --handshake $SERVE_DIR/handshake.json \
        --out $E6/results/e6.csv $bench_flags $BENCH_PSS_FLAG 2>&1 | tee -a "$PROGRESS" || note "mode=$mode N=$n P=$p FAILED"

    # Preserve the point's apiserver request log (the API-load source of
    # the idle table and the write counts of the ladder), named like the
    # driver's mode label.
    cp $SERVE_DIR/apiserver-requests.log "$E6/results/requests-$mode${BENCH_VARIANT:+-$BENCH_VARIANT}-$n-$p.log" 2>/dev/null || true

    cleanup
    trap cleanup EXIT
}

note "E6 sweep starting: modes=[$MODES] N=[$NS] P=[$PS] workloads=$WORKLOADS interval=${AUDIT_INTERVAL}s smith window ${SMITH_K_DEFAULT}s"
note "watch live: tail -f $PROGRESS"
# Phase 0: cold start, the loop closed from the first write (the bootstrap
# transient), baseline convergence only, up each mode's wall.
note "=== phase 0: cold start (N: $COLD_NS) ==="
for mode in $MODES; do
    case $mode in dpolicy*) ;; *) continue ;; esac
    for n in $COLD_NS; do
        BENCH_BOOTSTRAP=auto BENCH_VARIANT=cold TIMEOUT=$COLD_TIMEOUT run_point "$mode" "$n" 1 none
        grep -q "^$mode-cold,preload,$n," $E6/results/e6.csv 2>/dev/null ||
            { note "cold start of $mode at N=$n did not converge: the wall; larger sizes skipped"; break; }
    done
done
# Phase 1: the congestion ladder, w6 only: dpolicy at the realistic budget
# from the reconciler (K = 0) up the smith windows, the Gatekeeper
# contenders at their own.
note "=== phase 1: congestion ladder (N=$LADDER_N P=$LADDER_P, windows: 0 $SMITH_KS; dpolicy at $LADDER_QPS QPS) ==="
for mode in $MODES; do
    case $mode in
        dpolicy)
            SUT_QPS=$LADDER_QPS SUT_BURST=$LADDER_BURST BENCH_VARIANT=k0 REPS=$LADDER_REPS \
                run_point dpolicy "$LADDER_N" "$LADDER_P" w6 ;;
        dpolicy-smith)
            for K in $SMITH_KS; do
                SUT_QPS=$LADDER_QPS SUT_BURST=$LADDER_BURST SUT_SMITH_K=$K BENCH_VARIANT=k$K REPS=$LADDER_REPS \
                    run_point dpolicy-smith "$LADDER_N" "$LADDER_P" w6
            done ;;
        *)  REPS=$LADDER_REPS run_point "$mode" "$LADDER_N" "$LADDER_P" w6 ;;
    esac
done
# Phase 2: the grid; the idle workload only at the W2_PS points.
note "=== phase 2: grid (modes: $MODES; N: $NS; P: $PS) ==="
for mode in $MODES; do
    for n in $NS; do
        for p in $PS; do
            wl=$WORKLOADS
            case " $W2_PS " in *" $p "*) ;; *) wl=$(echo ",$wl," | sed 's/,w2,/,/; s/^,//; s/,$//') ;; esac
            run_point "$mode" "$n" "$p" "$wl"
        done
    done
done
# Phase 3: extra density for the W4 rollout curve: intermediate N points,
# w4 only, so the full workload set is not re-run at these sizes.
case ",$WORKLOADS," in *",w4,"*)
    note "=== phase 3: w4 density (N: $W4_NS; P: $W4_PS) ==="
    for mode in $MODES; do
        for n in $W4_NS; do
            for p in $W4_PS; do
                run_point "$mode" "$n" "$p" w4
            done
        done
    done ;;
esac
python3 $E6/summarize.py $E6/results | tee -a "$PROGRESS"
if [ "${RENDER:-yes}" = yes ]; then
    (cd $E6 && ../render.sh results.org) | tee -a "$PROGRESS" || note "render failed (rerun with 'make render')"
fi
note "E6 sweep DONE; results in $E6/results/ (e6.csv + derived series + request logs)"
