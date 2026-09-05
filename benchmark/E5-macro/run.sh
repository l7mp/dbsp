#!/usr/bin/env bash
# E5 sweep: the macrobenchmark. Every contender  -  delta-gateway in its
# four architectures (reconciler / open / sotw / smith) and stock envoy-gateway - runs
# OUT OF PROCESS against a fresh shared envtest plant (kube-apiserver +
# etcd) per grid point, driven and measured by the same driver: k8s writes
# through the plant kubeconfig, statuses watched back from the plant, xDS
# tapped with our delta-ADS clients at the contender's server. A /proc
# sampler and the apiserver request-audit log price each contender with
# the same instruments.
#
#   ./run.sh            fast profile (laptop)
#   SCALE=full ./run.sh paper profile (server)
#   EG=no ./run.sh      skip the envoy-gateway points
#   RENDER=no ./run.sh  skip chart rendering
#
# Phases: 1 convergence grid, 2 disturbance rejection, 3 corners,
# 4 embedded ablation + fan-out + storm, 5 derived tables + render.
set -eu
cd "$(dirname "$0")"

DBSP="${DBSP:-../../js/bin/dbsp}"
[ -x "$DBSP" ] || { echo "dbsp binary not found at $DBSP - run 'make build' first (or set DBSP)" >&2; exit 1; }
SCALE="${SCALE:-fast}"
RENDER="${RENDER:-yes}"
EG="${EG:-yes}"
EG_ROOT="${EG_ROOT:-../.cache/envoy-gateway}"
ISTIO="${ISTIO:-yes}"
ISTIO_ROOT="${ISTIO_ROOT:-../.cache/istio}"
ISTIO_BIN="${ISTIO_BIN:-$ISTIO_ROOT/bin/pilot-discovery}"
HARNESS=../plant/plant
CRD_DIR=../eg-harness/testdata/crds
PLANT_DIR=/tmp/gw-plant
EG_SERVE_DIR=/tmp/eg-serve
PROGRESS=/tmp/e5.log

if [ "$SCALE" = full ]; then
    RS_L4="${RS_L4:-18 40 73 150 270 520 1000 2000 4000 7000 10000}"
    # sotw is quadratic: the trend is established by 520 and r >= 1000 is a
    # proven 30-minute timeout per point.
    RS_SOTW="${RS_SOTW:-18 40 73 150 270 520}"
    RS_SHOP="${RS_SHOP:-5 10 15 22 32 44 54 68 82 100}"
    RS_EG="${RS_EG:-18 40 73 150 270 520 1000 2000 4000 7000 10000}"
    RS_ISTIO="${RS_ISTIO:-18 40 73 150 270 520 1000 2000 4000 7000 10000}"
    REPS="${REPS:-15}" REPS_SOTW="${REPS_SOTW:-3}" REPS_EG="${REPS_EG:-10}"
    # The dual-rate Smith window sweep, in seconds of wall time (the SUT
    # takes --smith-window in ms; the 1 s read-out clock quantizes it): a
    # window of K seconds absorbs bursts of up to K x 50 qps commands
    # silently. The window is the anti-windup lever, so the grid carries
    # one line per window.
    # SMITH_K_DEFAULT is the window every non-grid smith point (disturb
    # noise, corners, ablation) runs with.
    SMITH_KS="${SMITH_KS:-1 2 3 5 10}"
    SMITH_K_DEFAULT="${SMITH_K_DEFAULT:-10}"
    DISTURB_R="${DISTURB_R:-30}" ROUNDS="${ROUNDS:-300}" NOISES="${NOISES:-0.1 0.3 1}" BURST="${BURST:-20}"
    KS="${KS:-100 500 2000}" ES="${ES:-50 200 1000}" SAT_RATES="${SAT_RATES:-5 20 50 100}"
    SAT_DUR="${SAT_DUR:-60}" SAT_M="${SAT_M:-30}"
    AMP_NS="${AMP_NS:-200 1000 3000}" AMP_RATE="${AMP_RATE:-20}" AMP_DUR="${AMP_DUR:-30}"
    CONG_QPS="${CONG_QPS:-1 2 5 10 25 50}" CONG_BURST="${CONG_BURST:-30}"
    CLIENTS="${CLIENTS:-10 50 200 700}" FANOUT_R="${FANOUT_R:-73}" ABL_R="${ABL_R:-73}"
    TIMEOUT="${TIMEOUT:-1800000}" RUN_TIMEOUT="${RUN_TIMEOUT:-10800}"
else
    RS_L4="${RS_L4:-5 18 73}"
    RS_SOTW="${RS_SOTW:-5 18}"
    RS_SHOP="${RS_SHOP:-2 5}"
    RS_EG="${RS_EG:-5 18}"
    RS_ISTIO="${RS_ISTIO:-5 18}"
    REPS="${REPS:-8}" REPS_SOTW="${REPS_SOTW:-3}" REPS_EG="${REPS_EG:-5}"
    DISTURB_R="${DISTURB_R:-10}" ROUNDS="${ROUNDS:-50}" NOISES="${NOISES:-0.3}" BURST="${BURST:-10}"
    KS="${KS:-100 500}" ES="${ES:-50 200}" SAT_RATES="${SAT_RATES:-2 10}"
    SAT_DUR="${SAT_DUR:-20}" SAT_M="${SAT_M:-10}"
    AMP_NS="${AMP_NS:-50 200}" AMP_RATE="${AMP_RATE:-5}" AMP_DUR="${AMP_DUR:-10}"
    CONG_QPS="${CONG_QPS:-2 10}" CONG_BURST="${CONG_BURST:-10}"
    CLIENTS="${CLIENTS:-5 20 50}" FANOUT_R="${FANOUT_R:-18}" ABL_R="${ABL_R:-18}"
    TIMEOUT="${TIMEOUT:-120000}" RUN_TIMEOUT="${RUN_TIMEOUT:-1800}"
    SMITH_KS="${SMITH_KS:-2}"
    SMITH_K_DEFAULT="${SMITH_K_DEFAULT:-2}"
fi

note() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$PROGRESS"; }

rm -f results/*.csv results/requests-*.log
mkdir -p results
: > "$PROGRESS"

# --- Provisioning: the plant launcher, the EG clone + serve binary. ---
[ -x "$HARNESS" ] || (cd ../plant && GOWORK=off go build -o plant .)
export KUBEBUILDER_ASSETS="${KUBEBUILDER_ASSETS:-$(ls -d "$HOME"/.local/share/kubebuilder-envtest/k8s/*-linux-amd64 | sort -V | tail -1)}"
EG_BIN="$EG_ROOT/benchmarks/delta/delta.test"
if [ "$EG" = yes ]; then
    if [ ! -d "$EG_ROOT/.git" ]; then
        note "provisioning envoy-gateway clone (make eg)"
        make -C .. eg
    else
        # Refresh the harness overlay so eg-harness edits take effect.
        rm -rf "$EG_ROOT/benchmarks/delta"
        mkdir -p "$EG_ROOT/benchmarks"
        cp -a ../eg-harness "$EG_ROOT/benchmarks/delta"
    fi
    note "building the envoy-gateway serve binary"
    (cd "$EG_ROOT" && GOWORK=off go test -c -tags benchmark -o benchmarks/delta/delta.test ./benchmarks/delta)
fi
if [ "$ISTIO" = yes ] && [ ! -x "$ISTIO_BIN" ]; then
    note "provisioning istio (make istio)"
    make -C .. istio
fi

# --- Plant / SUT lifecycle. Every grid point gets a fresh plant. ---
PLANT_PID="" SUT_PID="" SAMPLER_PID=""
XDS_SEQ=0

cleanup() {
    touch $PLANT_DIR/stop $EG_SERVE_DIR/stop 2>/dev/null || true
    pkill -x pilot-discovery 2>/dev/null || true
    [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null || true
    [ -n "$SUT_PID" ] && kill "$SUT_PID" 2>/dev/null || true
    sleep 2
    [ -n "$PLANT_PID" ] && kill "$PLANT_PID" 2>/dev/null || true
    pkill -x delta.test 2>/dev/null || true
    sleep 1
    pkill -9 -x etcd 2>/dev/null || true
    pkill -9 -x kube-apiserver 2>/dev/null || true
    # A hard-killed control plane leaves its etcd data dir behind
    # (~130 MB per plant  -  a full sweep exhausts /tmp without this).
    rm -rf /tmp/k8s_test_framework_* 2>/dev/null || true
    PLANT_PID="" SUT_PID="" SAMPLER_PID=""
}
trap cleanup EXIT

# sampler <label> <pid>: one CSV line per second with the contender's
# cumulative CPU ticks (utime+stime) and RSS pages from /proc.
sampler() {
    local label=$1 pid=$2
    while kill -0 "$pid" 2>/dev/null; do
        awk -v l="$label" -v p="$pid" -v t="$(date +%s%3N)" \
            '{print t "," l "," p "," $14+$15 "," $24}' "/proc/$pid/stat" \
            >> results/proc.csv 2>/dev/null || true
        sleep 1
    done
}

boot_plant() {
    local sysmode=$1 label=$2
    rm -rf $PLANT_DIR $EG_SERVE_DIR
    # The istiod points add istio's own config CRDs and its root namespace.
    local extra=""
    if [ "$sysmode" = istio ]; then
        extra="-extra-crd-dir $ISTIO_ROOT/manifests/charts/base/files/crd-all.gen.yaml -create-ns istio-system"
    fi
    PLANT_DIR=$PLANT_DIR $HARNESS -crd-dir $CRD_DIR -gk=false $extra \
        > "/tmp/e5-plant-$label.log" 2>&1 &
    PLANT_PID=$!
    for i in $(seq 1 120); do [ -f $PLANT_DIR/handshake.json ] && break; sleep 1; done
    [ -f $PLANT_DIR/handshake.json ] || { note "plant failed, see /tmp/e5-plant-$label.log"; return 1; }
}

# start_sut <sysmode> <label>: spawn the contender against the plant.
# Sets SUT_PID (the /proc sampling target) and XDS_PORT/XDS_ADDR.
start_sut() {
    local sysmode=$1 label=$2
    XDS_SEQ=$((XDS_SEQ + 1))
    XDS_PORT=$((19000 + XDS_SEQ % 500))
    XDS_ADDR="127.0.0.1:$XDS_PORT"
    if [ "$sysmode" = eg ]; then
        # exec keeps $! pointing at delta.test itself (the sampler's and
        # kill's target), not an intermediate subshell.
        (cd "$EG_ROOT/benchmarks/delta" && \
            EG_SERVE_DIR=$EG_SERVE_DIR EG_KUBECONFIG=$PLANT_DIR/kubeconfig \
            exec ./delta.test -test.run TestServe -test.timeout 6h) \
            > "/tmp/e5-eg-$label.log" 2>&1 &
        SUT_PID=$!
        for i in $(seq 1 120); do [ -f $EG_SERVE_DIR/handshake.json ] && break; sleep 1; done
        [ -f $EG_SERVE_DIR/handshake.json ] || { note "eg serve failed, see /tmp/e5-eg-$label.log"; return 1; }
    elif [ "$sysmode" = istio ]; then
        # Stock pilot-discovery as a pure controller process: Gateway API
        # alpha kinds on (TCPRoute), the deployment controller off (manual
        # gateway mode: the driver provisions the per-gateway Services), no
        # webhook patching, telemetry providers off (their filter configs
        # are istio-proprietary protos), and the push debounce OFF  -  istiod
        # ships a 100 ms debounce floor in front of every xDS push, and the
        # benchmark measures its best case (set ISTIO_DEBOUNCE to restore
        # the as-deployed floor). Ports: xDS-over-gRPC on 15010
        # (plaintext), readiness on 8080.
        printf 'defaultProviders:\n  metrics: []\n' > /tmp/e5-meshconfig.yaml
        env KUBECONFIG=$PLANT_DIR/kubeconfig \
            PILOT_ENABLE_ALPHA_GATEWAY_API=true \
            PILOT_ENABLE_GATEWAY_API_DEPLOYMENT_CONTROLLER=false \
            VALIDATION_WEBHOOK_CONFIG_NAME= INJECTION_WEBHOOK_CONFIG_NAME= \
            CLUSTER_ID=Kubernetes \
            PILOT_DEBOUNCE_AFTER="${ISTIO_DEBOUNCE:-0ms}" \
            PILOT_DEBOUNCE_MAX="${ISTIO_DEBOUNCE_MAX:-0ms}" \
            "$ISTIO_BIN" discovery --kubeconfig $PLANT_DIR/kubeconfig \
            --meshConfig /tmp/e5-meshconfig.yaml \
            > "/tmp/e5-istiod-$label.log" 2>&1 &
        SUT_PID=$!
        XDS_ADDR="127.0.0.1:15010"
        for i in $(seq 1 60); do curl -fs 127.0.0.1:8080/ready >/dev/null 2>&1 && break; sleep 1; done
        curl -fs 127.0.0.1:8080/ready >/dev/null 2>&1 || { note "istiod failed, see /tmp/e5-istiod-$label.log"; return 1; }
    else
        KUBECONFIG=$PLANT_DIR/kubeconfig $DBSP ../../apps/dgateway/index.js controller \
            --mode "$sysmode" --xds-address "$XDS_ADDR" \
            ${SUT_QPS:+--qps "$SUT_QPS"} ${SUT_SMITH_K:+--smith-window "$((SUT_SMITH_K * 1000))"} \
            ${SUT_TICK:+--tick-period "$SUT_TICK"} \
            > "/tmp/e5-dgw-$label.log" 2>&1 &
        SUT_PID=$!
        sleep 2
    fi
    sampler "$label" "$SUT_PID" &
    SAMPLER_PID=$!
}

# point <script> <sysmode> <label> <driver args...>: one grid point on a
# fresh plant. Returns the driver's exit status (nonzero on a convergence
# timeout or a run-timeout kill), so callers can abort a sweep at the wall.
point() {
    local script=$1 sysmode=$2 label=$3
    shift 3
    local system=dgw
    case $sysmode in eg|istio) system=$sysmode ;; esac
    note "point $label: booting plant..."
    boot_plant "$sysmode" "$label" || return 0
    start_sut "$sysmode" "$label" || { cleanup; trap cleanup EXIT; return 0; }
    echo ">>> $script $label $*" | tee -a "$PROGRESS"
    timeout -k 30 "$RUN_TIMEOUT" "$DBSP" "$script" \
        --system="$system" --mode="$sysmode" --timeout-ms="$TIMEOUT" \
        --handshake=$PLANT_DIR/handshake.json --eg-handshake=$EG_SERVE_DIR/handshake.json \
        --xds-address="$XDS_ADDR" --sut-pid="$SUT_PID" "$@" 2>&1 |
        grep -iE "E5 |CORNER |preload:|converged|STATS |RUNTIME ERROR|error" | grep -v "channel full" | tee -a "$PROGRESS"
    local rc=${PIPESTATUS[0]}
    cp $PLANT_DIR/apiserver-requests.log "results/requests-$label.log" 2>/dev/null || true
    cleanup
    trap cleanup EXIT
    sleep 1
    if [ "$rc" -ne 0 ]; then
        note "point $label FAILED (rc=$rc)"
        return 1
    fi
    return 0
}

# dnf <context> <mode> <k> <scale> <outcome>: record a point the sweep could
# not complete (outcome timeout) or did not attempt (outcome skipped); the
# results doc renders this file as the feasibility table.
dnf() {
    if [ ! -f results/E5-DNF.csv ]; then
        echo "context,mode,k,scale,outcome" > results/E5-DNF.csv
    fi
    echo "$1,$2,$3,$4,$5" >> results/E5-DNF.csv
}

# sweep <script> <sysmode> <mode_label> <label_prefix> <trace> <reps> <k> <variant> <scales...>:
# an ascending scale sweep with early abort. The first failed point marks
# the wall; every larger scale is recorded as skipped, not run. A nonempty
# k starts the smith SUT with that window; a nonempty variant suffixes the
# driver's mode label (and so its CSV names) with -<variant>, so the
# default smith window keeps the plain "smith" line the charts reference.
sweep() {
    local script=$1 sysmode=$2 mlabel=$3 lprefix=$4 trace=$5 reps=$6 k=$7 variant=$8
    shift 8
    local failed=""
    local r
    for r in "$@"; do
        if [ -n "$failed" ]; then
            dnf "$trace" "$mlabel" "${k:-1}" "$r" skipped
            continue
        fi
        if ! SUT_SMITH_K=$k point "$script" "$sysmode" "$lprefix-r$r" \
                --trace="$trace" --regions="$r" --reps="$reps" ${variant:+--variant="$variant"}; then
            dnf "$trace" "$mlabel" "${k:-1}" "$r" timeout
            failed=1
        fi
    done
}

MODES="${MODES:-reconciler open sotw smith}"
[ "$EG" = yes ] && MODES="$MODES eg"
[ "$ISTIO" = yes ] && MODES="$MODES istio"

# --- Phase 1: convergence grid (per trace, per architecture, per scale).
# smith runs one grid line per compensation window (SMITH_KS); every sweep
# aborts at its wall (first timeout) and records the DNF tail. ---
note "=== phase 1: convergence grid (modes: $MODES, smith windows: $SMITH_KS) ==="
for m in $MODES; do
    case $m in
        smith) continue ;;
        sotw)  rs=$RS_SOTW;  reps=$REPS_SOTW ;;
        eg)    rs=$RS_EG;    reps=$REPS_EG ;;
        istio) rs=$RS_ISTIO; reps=$REPS_EG ;;
        *)     rs=$RS_L4;    reps=$REPS ;;
    esac
    sweep bench.js "$m" "$m" "mr-$m" multiregion "$reps" "" "" $rs
done
if [[ " $MODES " == *" smith "* ]]; then
    for K in $SMITH_KS; do
        v="k$K"
        [ "$K" = "$SMITH_K_DEFAULT" ] && v=""
        sweep bench.js smith "smith${v:+-$v}" "mr-smith-k$K" multiregion "$REPS" "$K" "$v" $RS_L4
    done
fi
for m in $MODES; do
    case $m in
        smith)    continue ;;
        sotw)     reps=$REPS_SOTW ;;
        eg|istio) reps=$REPS_EG ;;
        *)        reps=$REPS ;;
    esac
    sweep bench.js "$m" "$m" "shop-$m" sock-shop "$reps" "" "" $RS_SHOP
done
if [[ " $MODES " == *" smith "* ]]; then
    for K in $SMITH_KS; do
        v="k$K"
        [ "$K" = "$SMITH_K_DEFAULT" ] && v=""
        sweep bench.js smith "smith${v:+-$v}" "shop-smith-k$K" sock-shop "$REPS" "$K" "$v" $RS_SHOP
    done
fi

# --- Phase 2: disturbance rejection (status tampering). ---
DISTURB_MODES="${DISTURB_MODES:-reconciler open sotw smith}"
[ "$EG" = yes ] && DISTURB_MODES="$DISTURB_MODES eg"
[ "$ISTIO" = yes ] && DISTURB_MODES="$DISTURB_MODES istio"
note "=== phase 2: disturbance rejection (modes: $DISTURB_MODES) ==="
for m in $DISTURB_MODES; do
    k=""
    [ "$m" = smith ] && k=$SMITH_K_DEFAULT
    for l in $NOISES; do
        SUT_SMITH_K=$k point disturb.js "$m" "disturb-$m-l$l" --trace=multiregion \
            --regions="$DISTURB_R" --rounds="$ROUNDS" --noise="$l" || true
    done
    SUT_SMITH_K=$k point disturb.js "$m" "disturb-$m-burst" --trace=multiregion \
        --regions="$DISTURB_R" --rounds="$ROUNDS" --burst="$BURST" || true
done
# The window sweep on the burst point: rejection vs the compensation window
# on the real plant (the default window already ran in the loop above).
if [[ " $DISTURB_MODES " == *" smith "* ]]; then
    for K in $SMITH_KS; do
        [ "$K" = "$SMITH_K_DEFAULT" ] && continue
        SUT_SMITH_K=$K point disturb.js smith "disturb-smith-k$K-burst" --variant="k$K" \
            --trace=multiregion --regions="$DISTURB_R" --rounds="$ROUNDS" --burst="$BURST" || true
    done
fi

# --- Phase 2c: congestion  -  the loop behind a throttled client budget.
# The SUT's own k8s client is rate-limited, so its status corrections
# queue and the watch confirms them late: a real, tunable feedback dead
# time on a real apiserver. A burst of tamperings then compares the
# re-emitting reconciler loop against the exactly-once smith loop; phase 5
# counts each point's PATCH requests from the request-audit log. ---
note "=== phase 2c: congestion (reconciler vs smith, qps: $CONG_QPS) ==="
for m in reconciler smith; do
    for q in $CONG_QPS; do
        # The smith window tracks the throttle: the burst drains in
        # CONG_BURST/q seconds, and the window covers five drains.
        k=""
        [ "$m" = smith ] && k=$(( (5 * CONG_BURST + q - 1) / q ))
        SUT_QPS=$q SUT_SMITH_K=$k point congestion.js "$m" "cong-$m-q$q" --trace=multiregion \
            --regions="$DISTURB_R" --burst="$CONG_BURST" --qps="$q" || true
    done
done

# --- Phase 3: corners (mega-vhost, endpoint-heavy, saturation). smith
# rides along at the default window so dgw keeps data where the plain
# reconciler storms out; each ascending list aborts at its wall. ---
CORNER_MODES="${CORNER_MODES:-reconciler smith}"
[ "$EG" = yes ] && CORNER_MODES="$CORNER_MODES eg"
[ "$ISTIO" = yes ] && CORNER_MODES="$CORNER_MODES istio"
note "=== phase 3: corners (modes: $CORNER_MODES) ==="
for m in $CORNER_MODES; do
    reps=$REPS
    [ "$m" = sotw ] && reps=$REPS_SOTW
    smk=""
    [ "$m" = smith ] && smk=$SMITH_K_DEFAULT
    cfail=""
    for k in $KS; do
        if [ -n "$cfail" ]; then dnf mega-vhost "$m" "${smk:-1}" "$k" skipped; continue; fi
        SUT_SMITH_K=$smk point corner.js "$m" "mega-$m-k$k" --case=mega-vhost --routes="$k" --reps="$reps" ||
            { dnf mega-vhost "$m" "${smk:-1}" "$k" timeout; cfail=1; }
    done
    cfail=""
    for e in $ES; do
        if [ -n "$cfail" ]; then dnf endpoint-heavy "$m" "${smk:-1}" "$e" skipped; continue; fi
        SUT_SMITH_K=$smk point corner.js "$m" "heavy-$m-e$e" --case=endpoint-heavy --endpoints="$e" --reps="$reps" ||
            { dnf endpoint-heavy "$m" "${smk:-1}" "$e" timeout; cfail=1; }
    done
    cfail=""
    for rate in $SAT_RATES; do
        if [ -n "$cfail" ]; then dnf saturation "$m" "${smk:-1}" "$rate" skipped; continue; fi
        SUT_SMITH_K=$smk point corner.js "$m" "sat-$m-rate$rate" --case=saturation --rate="$rate" \
            --duration="$SAT_DUR" --gateways="$SAT_M" ||
            { dnf saturation "$m" "${smk:-1}" "$rate" timeout; cfail=1; }
    done
    for churn in annotation label; do
        cfail=""
        for n in $AMP_NS; do
            if [ -n "$cfail" ]; then dnf "amp-$churn" "$m" "${smk:-1}" "$n" skipped; continue; fi
            SUT_SMITH_K=$smk point corner.js "$m" "amp-$churn-$m-n$n" --case=amplification --routes="$n" \
                --churn="$churn" --rate="$AMP_RATE" --duration="$AMP_DUR" --reps=5 ||
                { dnf "amp-$churn" "$m" "${smk:-1}" "$n" timeout; cfail=1; }
        done
    done
done

# --- Phase 4: embedded ablation (no plant: the in-process controller on
# the embedded API server prices the real API path by contrast), plus the
# xDS fan-out and reconnect-storm scenarios that live on the same stack. ---
note "=== phase 4: embedded ablation + fan-out + storm ==="
PORT_SEQ=0
run_abl() {
    PORT_SEQ=$((PORT_SEQ + 1))
    echo ">>> ablation $*" | tee -a "$PROGRESS"
    timeout -k 30 "$RUN_TIMEOUT" "$DBSP" ablation.js --timeout-ms="$TIMEOUT" \
        --api-port=$((18500 + PORT_SEQ)) --xds-port=$((19700 + PORT_SEQ)) "$@" 2>&1 |
        grep -iE "E5 |preload:|STATS |RUNTIME ERROR|error" | grep -v "channel full" | tee -a "$PROGRESS" || true
    sleep 1
}
for m in reconciler open smith; do
    reps=$REPS
    [ "$m" = sotw ] && reps=$REPS_SOTW
    smk=""
    [ "$m" = smith ] && smk=$SMITH_K_DEFAULT
    run_abl --trace=multiregion --regions="$ABL_R" --mode="$m" --reps="$reps" \
        ${smk:+--smith-k="$smk"}
done
for c in $CLIENTS; do
    run_abl --trace=multiregion --regions="$FANOUT_R" --mode=reconciler --reps="$REPS" --clients="$c"
done
for c in $CLIENTS; do
    run_abl --trace=multiregion --regions="$FANOUT_R" --storm --clients="$c"
    run_abl --trace=multiregion --regions="$FANOUT_R" --storm --clients="$c" --stagger-ms=50
done

# --- Phase 5: derived chart tables. ---
# Preload (cold start): per architecture (smith window variants included)
# from the region-add CSVs (regions, preload_ms).
for f in results/E5-multiregion-*-region-add.csv; do
    [ -f "$f" ] || continue
    m=${f#results/E5-multiregion-}
    m=${m%-region-add.csv}
    awk -F, 'NR==1 {print "regions,preload_ms"} NR>1 {print $1","$7}' "$f" \
        > "results/E5-preload-$m.csv"
done
# Per-trace preload from the raw rows the driver writes before its
# changesets: E5-preload-<trace>-<mode>.csv keeps the cold start of the
# points whose changesets then timed out.
if [ -f results/E5-preload-raw.csv ]; then
    awk -F, 'NR>1 {
        f = "results/E5-preload-" $1 "-" $2 ".csv"
        if (!(f in seen)) { print "regions,preload_ms" > f; seen[f] = 1 }
        print $3 "," $4 >> f
    }' results/E5-preload-raw.csv
fi
# The feasibility table renders from E5-DNF.csv; guarantee it exists and
# derive the walls (the first timeout per line; skipped tails elided).
if [ ! -f results/E5-DNF.csv ]; then
    echo "context,mode,k,scale,outcome" > results/E5-DNF.csv
fi
awk -F, 'NR==1 || $5=="timeout"' results/E5-DNF.csv > results/E5-walls.csv
# Fan-out (one row per C) and the two storm variants, from the ablation.
{
    head -1 "$(ls results/E5-ablation-multiregion-reconciler-endpoint-update-c*.csv | head -1)"
    tail -q -n +2 results/E5-ablation-multiregion-reconciler-endpoint-update-c*.csv | sort -t, -k2 -n
} > results/E5-fanout-endpoint-update.csv 2>/dev/null || true
awk -F, 'NR==1 || $2==0' results/E5-storm-multiregion.csv > results/E5-storm-simultaneous.csv || true
awk -F, 'NR==1 || $2==50' results/E5-storm-multiregion.csv > results/E5-storm-staggered.csv || true

# Congestion: join heal times with the per-point PATCH counts from the
# request logs (SUT vs driver split by user agent).
python3 congestion_summary.py results | tee -a "$PROGRESS" || true

if [ "$RENDER" = yes ]; then
    ../render.sh results.org
fi
note "E5 sweep DONE; results in results/ (grid + disturb + corners + ablation + request logs)"
