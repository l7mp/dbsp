const DEFAULT_TOPICS = {
  inputs: {
    gatewayClass: "gwapi.input.gatewayclass",
    gateway: "gwapi.input.gateway",
    secret: "gwapi.input.secret",
    tcpRoute: "gwapi.input.tcproute",
  },
  outputs: {
    gatewayClassStatus: "gwapi.output.gatewayclass.status",
    gatewayStatus: "gwapi.output.gateway.status",
    gatewayClassStatusPatch: "gwapi.output.gatewayclass.status.patch",
    gatewayStatusPatch: "gwapi.output.gateway.status.patch",
  },
};

function withPrefix(prefix, topics = DEFAULT_TOPICS) {
  const out = { inputs: {}, outputs: {} };
  for (const [name, topic] of Object.entries(topics.inputs)) {
    out.inputs[name] = `${prefix}.${topic}`;
  }
  for (const [name, topic] of Object.entries(topics.outputs)) {
    out.outputs[name] = `${prefix}.${topic}`;
  }
  return out;
}

module.exports = {
  DEFAULT_TOPICS,
  withPrefix,
};
