package spec

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSpec(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Spec Suite")
}

// A dgateway-flavored operator: k8s sources, view targets, a status
// controller with a Reconciler, an xds-group target.
const operatorJSON = `{
  "controllers": [
    {
      "name": "input",
      "sources": [
        {"apiGroup": "gateway.networking.k8s.io", "kind": "Gateway"},
        {"apiGroup": "misc.connector.dcontroller.io", "kind": "Timer",
         "type": "Tick", "parameters": {"period": "5m", "name": "resync"}},
        {"apiGroup": "", "version": "v1", "kind": "Secret", "level": true,
         "labelSelector": {"matchLabels": {"managed": "true"}}}
      ],
      "pipeline": [[{"@inputs": ["Gateway"]}, {"@project": {"$.": "$."}}, {"@output": "GatewayView"}]],
      "targets": [{"kind": "GatewayView"}],
      "transforms": [{"name": "Incrementalizer"}]
    },
    {
      "name": "status",
      "sources": [{"kind": "GatewayView"}, {"kind": "GatewayStatusObserved"}],
      "pipeline": [[{"@inputs": ["GatewayView"]}, {"@project": {"$.": "$."}}, {"@output": "GatewayStatus"}]],
      "targets": [{"apiGroup": "gateway.networking.k8s.io", "kind": "Gateway", "type": "Patcher"}],
      "transforms": [
        {"name": "Incrementalizer"},
        {"name": "Reconciler", "pairs": [["GatewayStatusObserved", "GatewayStatus"]]}
      ]
    },
    {
      "name": "xds",
      "sources": [{"kind": "GatewayView"}],
      "pipeline": [[{"@inputs": ["GatewayView"]}, {"@project": {"$.": "$."}}, {"@output": "XdsListeners"}]],
      "targets": [{"apiGroup": "xds.connector.dcontroller.io", "kind": "Listener",
                   "parameters": {"address": ":18000"}}]
    }
  ]
}`

var _ = Describe("OperatorSpec", func() {
	It("round-trips through JSON", func() {
		var op OperatorSpec
		Expect(json.Unmarshal([]byte(operatorJSON), &op)).To(Succeed())
		Expect(op.Validate()).To(Succeed())

		b, err := json.Marshal(op)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(MatchJSON(operatorJSON))

		var again OperatorSpec
		Expect(json.Unmarshal(b, &again)).To(Succeed())
		b2, err := json.Marshal(again)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b2)).To(MatchJSON(operatorJSON))
	})

	It("deep-copies without aliasing", func() {
		var op OperatorSpec
		Expect(json.Unmarshal([]byte(operatorJSON), &op)).To(Succeed())
		cp := op.DeepCopy()
		(*cp.Controllers[0].Pipeline)[0] = 'X'
		cp.Controllers[0].Sources[0].Kind = "Changed"
		cp.Controllers[1].Transforms[1].Pairs[0][0] = "changed"
		Expect(string(*op.Controllers[0].Pipeline)).To(HavePrefix("["))
		Expect(op.Controllers[0].Sources[0].Kind).To(Equal("Gateway"))
		Expect(op.Controllers[1].Transforms[1].Pairs[0][0]).To(Equal("GatewayStatusObserved"))
	})

	It("validates the structural invariants", func() {
		bad := func(src, msg string) {
			GinkgoHelper()
			var op OperatorSpec
			Expect(json.Unmarshal([]byte(src), &op)).To(Succeed())
			Expect(op.Validate()).To(MatchError(ContainSubstring(msg)))
		}
		bad(`{"controllers": []}`, "at least one controller")
		bad(`{"controllers": [{"sources": [], "targets": []}]}`, "name is required")
		bad(`{"controllers": [{"name": "c", "sources": [], "targets": []}]}`, "exactly one of pipeline, sql, or circuit")
		bad(`{"controllers": [{"name": "c", "pipeline": [], "sql": [], "sources": [], "targets": []}]}`, "exactly one of pipeline, sql, or circuit")
		bad(`{"controllers": [{"name": "c", "pipeline": [], "sources": [{}], "targets": []}]}`, "kind is required")
		bad(`{"controllers": [{"name": "c", "pipeline": [], "sources": [], "targets": [],
		     "transforms": [{"name": "NoSuch"}]}]}`, "unknown transformer")
	})
})
