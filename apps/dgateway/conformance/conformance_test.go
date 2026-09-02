// Package conformance runs the upstream Gateway API conformance suite
// against the delta-gateway operator. The test needs the full local stack
// (envtest control plane, operator, Envoy, fake infra) provided by the
// harness; run it through run.sh, which builds everything and executes this
// test inside the harness. The suite is configured entirely through the
// upstream conformance flags (--gateway-class, --supported-features,
// --skip-tests, --run-test, ...), which run.sh assembles.
package conformance

import (
	"os"
	"testing"

	"sigs.k8s.io/gateway-api/conformance"
)

func TestConformance(t *testing.T) {
	if os.Getenv("DELTA_GATEWAY_CONFORMANCE") == "" {
		t.Skip("set DELTA_GATEWAY_CONFORMANCE=1 (use run.sh) to run the conformance suite")
	}
	conformance.RunConformance(t)
}
