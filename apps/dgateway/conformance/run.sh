#!/usr/bin/env bash
# Run the Gateway API conformance suite against delta-gateway on localhost.
#
# No cluster and no root needed: the stack (envtest kube-apiserver+etcd, the
# dbsp operator, Envoy, fake echo backends) runs inside a user+network
# namespace (`unshare -r -n`) where binding the privileged listener ports
# (80, 443) on loopback is allowed.
#
# Usage:
#   ./run.sh                                  # the default conformance run
#   ./run.sh --run-test HTTPRouteSimpleSameNamespace
#   ./run.sh --skip-tests ...  --debug=true   # any upstream conformance flag
#
# Extra arguments are appended to the test binary invocation and can override
# the defaults assembled below.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

# ---------------------------------------------------------------------------
# Inner mode: we are already inside the user+network namespace.
if [[ "${1:-}" == "--inner" ]]; then
  shift
  ip link set lo up
  # A loopback default route: everything lives on 127.0.0.0/8, but some
  # components (kube-apiserver address detection) want a route table entry.
  ip route add default dev lo 2>/dev/null || true
  # The echo backends need a non-loopback address: the API server rejects
  # loopback endpoint addresses in EndpointSlices.
  ip link add dbsp0 type dummy 2>/dev/null || true
  ip addr add 10.44.0.1/24 dev dbsp0 2>/dev/null || true
  ip link set dbsp0 up
  set +e
  "$SCRIPT_DIR/bin/harness" \
    -repo-root "$REPO_ROOT" \
    -crd-dir "$DELTA_GW_CRD_DIR" \
    -envtest-bin "$DELTA_GW_ENVTEST_BIN" \
    -dgw-mode "${DGW_MODE:-reconciler}" \
    -- \
    "$SCRIPT_DIR/bin/conformance.test" -test.v -test.timeout 60m "$@" \
    2>&1 | tee "$SCRIPT_DIR/conformance.log"
  status=${PIPESTATUS[0]}
  set -e
  # RESULTS.md: per-test verdicts of the run just made, tracked so the
  # pass/skip surface is inspectable without rerunning the suite. Tests
  # skip when they need a feature outside --supported-features.
  {
    echo "# Gateway API conformance results"
    echo
    echo "Verdicts of the last local run (\`run.sh\`; full log in"
    echo "\`conformance.log\`, untracked). Skipped tests require features"
    echo "outside the declared \`--supported-features\` set."
    echo
    for verdict in PASS FAIL SKIP; do
      tests=$(grep -E "^    --- $verdict: TestConformance/" "$SCRIPT_DIR/conformance.log" \
        | sed -E 's|^    --- [A-Z]+: TestConformance/||; s/ \([0-9.]+s\)$//' | sort || true)
      [ -n "$tests" ] || continue
      echo "## $verdict ($(echo "$tests" | wc -l))"
      echo
      echo "$tests" | sed 's/^/- /'
      echo
    done
  } > "$SCRIPT_DIR/RESULTS.md"
  echo ">>> per-test verdicts written to $SCRIPT_DIR/RESULTS.md"
  exit "$status"
fi

# ---------------------------------------------------------------------------
# Outer mode: build everything (needs network for module downloads), then
# re-exec ourselves inside the namespace.

echo ">>> building dbsp runtime"
(cd "$REPO_ROOT/js" && go build -o bin/dbsp ./cmd)

echo ">>> building harness and conformance test binary"
cd "$SCRIPT_DIR"
mkdir -p bin
GOWORK=off go build -o bin/harness ./harness
GOWORK=off go test -c -o bin/conformance.test .

DELTA_GW_CRD_DIR="$(GOWORK=off go list -m -f '{{.Dir}}' sigs.k8s.io/gateway-api)/config/crd/experimental"
DELTA_GW_ENVTEST_BIN=$(ls -d "$HOME"/.local/share/kubebuilder-envtest/k8s/*-linux-amd64 | sort -V | tail -1)
export DELTA_GW_CRD_DIR DELTA_GW_ENVTEST_BIN

# The features delta-gateway implements. Tests requiring anything outside
# this set are skipped by the suite itself. The extended HTTPRoute
# features (method, query-parameter and regex matching, scheme/port/path
# redirects, URL rewrites, response-header modification) are rejected as
# UnsupportedValue by the current RDS translation and stay undeclared.
SUPPORTED_FEATURES=(
  Gateway
  GatewayPort8080
  HTTPRoute
)
FEATURES=$(IFS=,; echo "${SUPPORTED_FEATURES[*]}")

DEFAULT_ARGS=(
  --gateway-class=delta-gateway
  --supported-features="$FEATURES"
  # envtest has no namespace controller, so deleted namespaces would hang in
  # Terminating; the control plane is torn down after the run anyway.
  --cleanup-base-resources=false
)

echo ">>> entering user+network namespace"
exec unshare -r -n "$0" --inner "${DEFAULT_ARGS[@]}" "$@"
