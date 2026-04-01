const {
  clone,
  gatewayClassDoc,
  gatewayClassOtherDoc,
  statusRecord,
} = require("./fixtures.js");

function gwclassCases(topics) {
  return [
    {
      name: "gwclass: accepted true for owned class",
      inputs: {
        [topics.inputs.gatewayClass]: [clone(gatewayClassDoc())],
      },
      expected: {
        [topics.outputs.gatewayClassStatus]: [
          statusRecord(
            {
              apiVersion: "gateway.networking.k8s.io/v1",
              kind: "GatewayClass",
              name: "dbsp-gateway-class",
            },
            "Accepted",
            "True",
            "Accepted",
          ),
        ],
      },
    },
    {
      name: "gwclass: ignores foreign controller class",
      inputs: {
        [topics.inputs.gatewayClass]: [clone(gatewayClassOtherDoc())],
      },
      expected: {
        [topics.outputs.gatewayClassStatus]: [],
      },
    },
    {
      name: "gwclass: handles mixed classes",
      inputs: {
        [topics.inputs.gatewayClass]: [clone(gatewayClassDoc()), clone(gatewayClassOtherDoc())],
      },
      expected: {
        [topics.outputs.gatewayClassStatus]: [
          statusRecord(
            {
              apiVersion: "gateway.networking.k8s.io/v1",
              kind: "GatewayClass",
              name: "dbsp-gateway-class",
            },
            "Accepted",
            "True",
            "Accepted",
          ),
        ],
      },
    },
  ];
}

module.exports = {
  gwclassCases,
};
