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

// A dgateway-flavored runtime: k8s, misc and view sources, an internal
// view stream between the circuits, a Patcher target named apart from
// its watched kind, an xds-group target.
const runtimeJSON = `{
  "sources": [
    {"apiGroup": "gateway.networking.k8s.io", "kind": "Gateway"},
    {"apiGroup": "misc.connector.dcontroller.io", "kind": "Timer",
     "type": "Tick", "parameters": {"period": "5m", "name": "resync"}},
    {"apiGroup": "", "version": "v1", "kind": "Secret",
     "labelSelector": {"matchLabels": {"managed": "true"}}},
    {"kind": "GatewayStatusObserved"}
  ],
  "circuits": [
    {
      "name": "input",
      "inputs": ["Gateway", "Timer", "Secret"],
      "outputs": ["GatewayView"],
      "pipeline": [[{"@inputs": ["Gateway"]}, {"@project": {"$.": "$."}}, {"@output": "GatewayView"}]],
      "transforms": [{"name": "Incrementalizer"}]
    },
    {
      "name": "status",
      "inputs": ["GatewayView", "GatewayStatusObserved"],
      "outputs": ["GatewayStatus"],
      "pipeline": [[{"@inputs": ["GatewayView"]}, {"@project": {"$.": "$."}}, {"@output": "GatewayStatus"}]],
      "transforms": [
        {"name": "Incrementalizer"},
        {"name": "Reconciler", "pairs": [["GatewayStatusObserved", "GatewayStatus"]]}
      ]
    },
    {
      "name": "xds",
      "inputs": ["GatewayView"],
      "outputs": ["XdsListeners"],
      "pipeline": [[{"@inputs": ["GatewayView"]}, {"@project": {"$.": "$."}}, {"@output": "XdsListeners"}]]
    }
  ],
  "targets": [
    {"apiGroup": "gateway.networking.k8s.io", "kind": "Gateway", "type": "Patcher", "as": "GatewayStatus"},
    {"apiGroup": "xds.connector.dcontroller.io", "kind": "Listener", "as": "XdsListeners",
     "parameters": {"address": ":18000"}}
  ]
}`

var _ = Describe("RuntimeSpec", func() {
	It("round-trips through JSON", func() {
		var op RuntimeSpec
		Expect(json.Unmarshal([]byte(runtimeJSON), &op)).To(Succeed())
		Expect(op.Validate()).To(Succeed())

		b, err := json.Marshal(op)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(MatchJSON(runtimeJSON))

		var again RuntimeSpec
		Expect(json.Unmarshal(b, &again)).To(Succeed())
		b2, err := json.Marshal(again)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b2)).To(MatchJSON(runtimeJSON))
	})

	It("deep-copies without aliasing", func() {
		var op RuntimeSpec
		Expect(json.Unmarshal([]byte(runtimeJSON), &op)).To(Succeed())
		cp := op.DeepCopy()
		(*cp.Circuits[0].Pipeline)[0] = 'X'
		cp.Sources[0].Kind = "Changed"
		cp.Circuits[0].Inputs[0] = "changed"
		cp.Circuits[1].Transforms[1].Pairs[0][0] = "changed"
		cp.Targets[0].As = "changed"
		Expect(string(*op.Circuits[0].Pipeline)).To(HavePrefix("["))
		Expect(op.Sources[0].Kind).To(Equal("Gateway"))
		Expect(op.Circuits[0].Inputs[0]).To(Equal("Gateway"))
		Expect(op.Circuits[1].Transforms[1].Pairs[0][0]).To(Equal("GatewayStatusObserved"))
		Expect(op.Targets[0].As).To(Equal("GatewayStatus"))
	})

	It("validates the structural invariants", func() {
		bad := func(src, msg string) {
			GinkgoHelper()
			var op RuntimeSpec
			Expect(json.Unmarshal([]byte(src), &op)).To(Succeed())
			Expect(op.Validate()).To(MatchError(ContainSubstring(msg)))
		}
		bad(`{"circuits": []}`, "at least one circuit")
		bad(`{"circuits": [{}]}`, "name is required")
		bad(`{"circuits": [{"name": "c"}]}`, "exactly one of pipeline, sql, or graph")
		bad(`{"circuits": [{"name": "c", "pipeline": [], "sql": []}]}`, "exactly one of pipeline, sql, or graph")
		bad(`{"circuits": [{"name": "c", "pipeline": []}, {"name": "c", "graph": []}]}`, "duplicate name")
		bad(`{"circuits": [{"name": "c", "pipeline": []}], "sources": [{}]}`, "kind is required")
		bad(`{"circuits": [{"name": "c", "pipeline": []}], "targets": [{}]}`, "kind is required")
	})
})
