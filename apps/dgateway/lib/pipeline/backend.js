// Backend processing, up to the BackendView: Service ports, their ready
// endpoints from the EndpointSlices, and the upstream TLS requirement from
// BackendTLSPolicy, folded into one row per (Service, port).

function branches() {
  return [
    // One row per (Service, port).
    [
      { "@inputs": ["Service"] },
      { "@unwind": "$.spec.ports" },
      {
        "@project": {
          service: { name: "$.metadata.name", namespace: "$.metadata.namespace" },
          portName: { "@definedOr": ["$.spec.ports.name", ""] },
          port: "$.spec.ports.port",
        },
      },
      { "@output": "servicePorts" },
    ],

    // Ready endpoints per (service, port name): the EndpointSlices unwound
    // to one row per ready address (the slice's service-name label names
    // the Service), aggregated back per port.
    [
      { "@inputs": ["EndpointSlice"] },
      { "@unwind": "$.ports" },
      { "@unwind": "$.endpoints" },
      { "@unwind": "$.endpoints.addresses" },
      { "@select": { "@eq": [{ "@definedOr": ["$.endpoints.conditions.ready", true] }, true] } },
      {
        "@groupBy": [
          {
            service: {
              name: '$["metadata"]["labels"]["kubernetes.io/service-name"]',
              namespace: "$.metadata.namespace",
            },
            portName: { "@definedOr": ["$.ports.name", ""] },
          },
          { address: "$.endpoints.addresses", port: "$.ports.port" },
          { distinct: true },
        ],
      },
      {
        "@project": {
          service: "$.key.service",
          portName: "$.key.portName",
          endpoints: "$.values",
        },
      },
      { "@output": "serviceEndpoints" },
    ],

    // Upstream TLS requirements per backend Service, from BackendTLSPolicy.
    // Only System well-known CA certificates are supported: a policy using
    // caCertificateRefs cannot be honored, and its clusters are suppressed
    // (fail closed) rather than programmed plaintext. Policy status is not
    // reported.
    [
      { "@inputs": ["BackendTLSPolicy"] },
      { "@unwind": "$.spec.targetRefs" },
      {
        "@project": {
          service: { name: "$.spec.targetRefs.name", namespace: "$.metadata.namespace" },
          sni: { "@definedOr": ["$.spec.validation.hostname", ""] },
          supported: {
            "@and": [
              {
                "@eq": [
                  { "@definedOr": ["$.spec.validation.wellKnownCACertificates", ""] },
                  "System",
                ],
              },
              {
                "@eq": [
                  { "@len": [{ "@definedOr": ["$.spec.validation.caCertificateRefs", []] }] },
                  0,
                ],
              },
            ],
          },
        },
      },
      { "@output": "backendTLSPolicies" },
    ],

    // Service ports with their upstream TLS requirement (soft: most ports
    // have no policy). A policy that cannot be honored marks the row
    // tlsOk=false; the xDS output suppresses those clusters.
    [
      { "@inputs": ["servicePorts", "backendTLSPolicies"] },
      {
        "@join": [
          { "@eq": ["$.servicePorts.service", "$.backendTLSPolicies.service"] },
          {
            soft: ["backendTLSPolicies"],
            index: { servicePorts: "$.service", backendTLSPolicies: "$.service" },
          },
        ],
      },
      {
        "@project": [
          { "$.": "$.servicePorts" },
          {
            tlsOk: {
              "@or": [
                { "@isnil": "$.backendTLSPolicies" },
                { "@eq": ["$.backendTLSPolicies.supported", true] },
              ],
            },
            hasTls: { "@not": { "@isnil": "$.backendTLSPolicies" } },
            sni: { "@definedOr": ["$.backendTLSPolicies.sni", ""] },
          },
        ],
      },
      { "@output": "servicePortsTLS" },
    ],

    // BackendView: one row per (Service, port), endpoints folded in (soft:
    // a port with no ready endpoints keeps an empty list).
    [
      { "@inputs": ["servicePortsTLS", "serviceEndpoints"] },
      {
        "@join": [
          {
            "@and": [
              { "@eq": ["$.servicePortsTLS.service", "$.serviceEndpoints.service"] },
              { "@eq": ["$.servicePortsTLS.portName", "$.serviceEndpoints.portName"] },
            ],
          },
          {
            soft: ["serviceEndpoints"],
            index: {
              servicePortsTLS: { service: "$.service", portName: "$.portName" },
              serviceEndpoints: { service: "$.service", portName: "$.portName" },
            },
          },
        ],
      },
      {
        "@project": {
          metadata: {
            name: { "@hash": { service: "$.servicePortsTLS.service", port: "$.servicePortsTLS.port" } },
            namespace: "$.servicePortsTLS.service.namespace",
          },
          service: "$.servicePortsTLS.service",
          port: "$.servicePortsTLS.port",
          tlsOk: "$.servicePortsTLS.tlsOk",
          hasTls: "$.servicePortsTLS.hasTls",
          sni: "$.servicePortsTLS.sni",
          endpoints: { "@definedOr": ["$.serviceEndpoints.endpoints", []] },
        },
      },
      { "@output": "BackendView" },
    ],
  ];
}

module.exports = { branches };
