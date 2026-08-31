// Gateway processing, up to the GatewayView: one curated row per listener
// of every gateway owned by an accepted class of ours - the listener's
// admission verdict, allowedRoutes policy, resolved TLS material and RDS
// name - and the per-gateway view that regroups them with their
// attached-route counts.
//
// The status writer (statusBranches, k8s output layer) renders the view
// into Gateway status documents.

const { GATEWAY_API_GROUP } = require("../config.js");
const { SUPPORTED_PROTOCOLS, gatewayAddressExpr, transitionStamp } = require("./expr.js");

// The expressions below are evaluated on a (listenerRows, secrets) join
// row; the raw listener spec is $.listenerRows.listener.value.
const listenerAccepted = { "@in": ["$.listenerRows.listener.value.protocol", SUPPORTED_PROTOCOLS] };

// The listener's allowedRoutes.kinds handling. Accepted (HTTP/HTTPS)
// listeners admit HTTPRoute and nothing else, listeners of an unsupported
// protocol admit no kind at all: a requested kind is valid when its group
// is the Gateway API group (or unset), its kind is HTTPRoute and the
// listener is accepted; the listener's supported kinds are the valid
// requested kinds, or the protocol's kinds when none are requested.
// supportedKinds drives both the listener status and route-attachment
// admission; invalid requested kinds surface as
// ResolvedRefs=False/InvalidRouteKinds.
const requestedKinds = { "@definedOr": ["$.listenerRows.listener.value.allowedRoutes.kinds", []] };
const noKindsRequested = { "@eq": [{ "@len": [requestedKinds] }, 0] };
const protocolKinds = {
  "@cond": [listenerAccepted, [{ group: GATEWAY_API_GROUP, kind: "HTTPRoute" }], []],
};
const validRequestedKinds = {
  "@filter": [
    {
      "@and": [
        { "@eq": [{ "@definedOr": ["$$.group", GATEWAY_API_GROUP] }, GATEWAY_API_GROUP] },
        { "@eq": ["$$.kind", "HTTPRoute"] },
        listenerAccepted,
      ],
    },
    requestedKinds,
  ],
};
const supportedKinds = {
  "@cond": [
    noKindsRequested,
    protocolKinds,
    { "@map": [{ group: GATEWAY_API_GROUP, kind: "$$.kind" }, validRequestedKinds] },
  ],
};
const kindsResolved = {
  "@or": [
    noKindsRequested,
    { "@eq": [{ "@len": [validRequestedKinds] }, { "@len": [requestedKinds] }] },
  ],
};

// Certificate resolution for HTTPS listeners: exactly one core-group Secret
// certificateRef in the gateway's own namespace is supported (no
// ReferenceGrant), and it must resolve to a kubernetes.io/tls Secret
// carrying both tls.crt and tls.key. A malformed payload is an invalid
// certificateRef. The check is a PEM header sniff on the still-base64
// payloads: "LS0tLS1CRUdJTiBD" is base64("-----BEGIN C"...), the prefix
// of every PEM certificate block, and "LS0tLS1CRUdJ" is
// base64("-----BEGI"...), shared by every PEM key type.
const certResolved = {
  "@and": [
    { "@eq": [{ "@len": [{ "@definedOr": ["$.listenerRows.listener.value.tls.certificateRefs", []] }] }, 1] },
    { "@eq": [{ "@definedOr": ["$.listenerRows.listener.value.tls.certificateRefs[0].kind", "Secret"] }, "Secret"] },
    { "@eq": [{ "@definedOr": ["$.listenerRows.listener.value.tls.certificateRefs[0].group", ""] }, ""] },
    { "@not": { "@isnil": "$.secrets" } },
    { "@eq": [{ "@definedOr": ["$.secrets.type", ""] }, "kubernetes.io/tls"] },
    { "@startswith": [{ "@definedOr": ["$.secrets.certChain", ""] }, "LS0tLS1CRUdJTiBD"] },
    { "@startswith": [{ "@definedOr": ["$.secrets.privateKey", ""] }, "LS0tLS1CRUdJ"] },
  ],
};

// The RDS route-configuration name of a listener. Same-port HTTP listeners
// merge into one Envoy listener sharing a per-port configuration
// (virtual-host domains implement per-listener hostnames); HTTPS listeners
// each get an SNI-matched filter chain with a per-listener configuration.
const rdsName = {
  "@cond": [
    { "@eq": ["$.listenerRows.listener.value.protocol", "HTTP"] },
    { "@concat": ["$.listenerRows.gateway.namespace", "/", "$.listenerRows.gateway.name", "/", "$.listenerRows.listener.value.port"] },
    { "@concat": ["$.listenerRows.gateway.namespace", "/", "$.listenerRows.gateway.name", "/", "$.listenerRows.listener.value.name"] },
  ],
};

function branches({ controllerName, addressPool }) {
  return [
    // Raw per-listener rows. Gateways referencing a foreign or missing
    // class produce no rows (and therefore no status).
    [
      { "@inputs": ["GatewayClass", "Gateway"] },
      {
        "@join": [
          {
            "@and": [
              { "@eq": ["$.GatewayClass.metadata.name", "$.Gateway.spec.gatewayClassName"] },
              { "@eq": ["$.GatewayClass.spec.controllerName", controllerName] },
            ],
          },
          {
            index: {
              GatewayClass: "$.metadata.name",
              Gateway: "$.spec.gatewayClassName",
            },
          },
        ],
      },
      {
        "@project": {
          gateway: {
            name: "$.Gateway.metadata.name",
            namespace: "$.Gateway.metadata.namespace",
          },
          generation: { "@definedOr": ["$.Gateway.metadata.generation", 1] },
          // No parameter kinds are supported: any infrastructure
          // parametersRef is invalid and rejects the whole Gateway
          // (Accepted: False/InvalidParameters).
          invalidParams: { "@exists": "$.Gateway.spec.infrastructure.parametersRef" },
          // Every listener rides as the enumerated {index, value} pair:
          // the position in the gateway spec is the sort key of the
          // per-gateway regroup below.
          listener: { "@enumerate": "$.Gateway.spec.listeners" },
        },
      },
      { "@unwind": "$.listener" },
      { "@output": "listenerRows" },
    ],

    // TLS secrets, with the certificate/key payloads. Secret data values are
    // base64-encoded, which is exactly what Envoy's inlineBytes wants, so
    // they pass through untouched.
    [
      { "@inputs": ["Secret"] },
      {
        "@project": {
          secret: { name: "$.metadata.name", namespace: "$.metadata.namespace" },
          type: { "@definedOr": ["$.type", ""] },
          certChain: { "@definedOr": ['$["data"]["tls.crt"]', ""] },
          privateKey: { "@definedOr": ['$["data"]["tls.key"]', ""] },
        },
      },
      { "@output": "secrets" },
    ],

    // The curated listener rows: the raw rows with their certificateRef
    // resolved against the Secrets (soft: the Secret may be missing, and
    // HTTP listeners reference none). Listeners of an unsupported protocol
    // are kept, rejected (accepted: false), so they still get a status.
    [
      { "@inputs": ["listenerRows", "secrets"] },
      {
        "@join": [
          {
            "@and": [
              { "@eq": ["$.listenerRows.listener.value.tls.certificateRefs[0].name", "$.secrets.secret.name"] },
              { "@eq": ["$.listenerRows.gateway.namespace", "$.secrets.secret.namespace"] },
            ],
          },
          {
            soft: ["secrets"],
            index: {
              listenerRows: {
                name: { "@definedOr": ["$.listener.value.tls.certificateRefs[0].name", ""] },
                namespace: "$.gateway.namespace",
              },
              secrets: { name: "$.secret.name", namespace: "$.secret.namespace" },
            },
          },
        ],
      },
      {
        "@project": {
          gateway: "$.listenerRows.gateway",
          generation: "$.listenerRows.generation",
          invalidParams: "$.listenerRows.invalidParams",
          listener: {
            index: "$.listenerRows.listener.index",
            name: "$.listenerRows.listener.value.name",
            port: "$.listenerRows.listener.value.port",
            protocol: "$.listenerRows.listener.value.protocol",
            hostname: { "@definedOr": ["$.listenerRows.listener.value.hostname", ""] },
            accepted: listenerAccepted,
            reason: { "@cond": [listenerAccepted, "Accepted", "UnsupportedProtocol"] },
            fromNamespaces: {
              "@definedOr": ["$.listenerRows.listener.value.allowedRoutes.namespaces.from", "Same"],
            },
            nsSelector: {
              "@definedOr": ["$.listenerRows.listener.value.allowedRoutes.namespaces.selector", {}],
            },
            supportedKinds: supportedKinds,
            kindsResolved: kindsResolved,
            // An HTTPS listener with an unresolved certificateRef is not
            // certOk and is not programmed; HTTP listeners need no
            // certificate.
            certOk: {
              "@cond": [
                { "@neq": ["$.listenerRows.listener.value.protocol", "HTTPS"] },
                true,
                certResolved,
              ],
            },
            certChain: { "@definedOr": ["$.secrets.certChain", ""] },
            privateKey: { "@definedOr": ["$.secrets.privateKey", ""] },
            rdsName: rdsName,
          },
        },
      },
      { "@output": "listeners" },
    ],

    // Attached-route count per listener (distinct routes over the
    // attachment facet of routeFacets, httproute.js).
    [
      { "@inputs": ["routeFacets"] },
      { "@select": { "@eq": ["$.facet", "attachment"] } },
      {
        "@groupBy": [
          { gateway: "$.attachment.gateway", listenerName: "$.attachment.listener.name" },
          "$.route",
          { distinct: true },
        ],
      },
      {
        "@project": {
          gateway: "$.key.gateway",
          listenerName: "$.key.listenerName",
          attachedRoutes: { "@len": "$.values" },
        },
      },
      { "@output": "listenerCounts" },
    ],

    // GatewayView: one row per Gateway, the listener rows with their
    // attached-route counts (soft: listeners with no attached routes default
    // to 0) regrouped per gateway in spec order. In local address-pool mode
    // every gateway is assigned its own deterministic loopback data-plane
    // address, reported in the Gateway status and bound by its Envoy
    // listeners.
    [
      { "@inputs": ["listeners", "listenerCounts"] },
      {
        "@join": [
          {
            "@and": [
              { "@eq": ["$.listeners.gateway", "$.listenerCounts.gateway"] },
              { "@eq": ["$.listeners.listener.name", "$.listenerCounts.listenerName"] },
            ],
          },
          {
            soft: ["listenerCounts"],
            index: {
              listeners: { gateway: "$.gateway", listenerName: "$.listener.name" },
              listenerCounts: { gateway: "$.gateway", listenerName: "$.listenerName" },
            },
          },
        ],
      },
      {
        // The per-gateway columns ride in the group KEY: every listener
        // row of a gateway carries the same generation/invalidParams (and
        // the address is a function of the gateway name), so they never
        // split a group; the key is simply how group-constant fields reach
        // the projection after a @groupBy ($.key.*), the same idiom as in
        // the status writers.
        "@groupBy": [
          {
            gateway: "$.listeners.gateway",
            generation: "$.listeners.generation",
            invalidParams: "$.listeners.invalidParams",
            ...(addressPool
              ? { address: gatewayAddressExpr(addressPool, "$.listeners.gateway") }
              : {}),
          },
          {
            index: "$.listeners.listener.index",
            name: "$.listeners.listener.name",
            port: "$.listeners.listener.port",
            protocol: "$.listeners.listener.protocol",
            hostname: "$.listeners.listener.hostname",
            accepted: "$.listeners.listener.accepted",
            reason: "$.listeners.listener.reason",
            supportedKinds: "$.listeners.listener.supportedKinds",
            kindsResolved: "$.listeners.listener.kindsResolved",
            certOk: "$.listeners.listener.certOk",
            certChain: "$.listeners.listener.certChain",
            privateKey: "$.listeners.listener.privateKey",
            rdsName: "$.listeners.listener.rdsName",
            attachedRoutes: { "@definedOr": ["$.listenerCounts.attachedRoutes", 0] },
          },
          { distinct: true },
        ],
      },
      {
        "@project": {
          metadata: { name: "$.key.gateway.name", namespace: "$.key.gateway.namespace" },
          gateway: "$.key.gateway",
          generation: "$.key.generation",
          invalidParams: "$.key.invalidParams",
          ...(addressPool ? { address: "$.key.address" } : {}),
          // The listener entries carry their spec position (index), so the
          // spec order is a plain stable sort.
          listeners: { "@sortByKey": ["$$.index", "$.values"] },
        },
      },
      { "@output": "GatewayView" },
    ],
  ];
}

// ---------------------------------------------------------------------
// Gateway status writer: renders GatewayView rows into Gateway status
// documents - gateway-level conditions plus one status entry per listener,
// in spec order. The view row is flattened to one row per listener, every
// condition's lastTransitionTime is sampled by @stamp keyed on that
// condition's status (per gateway, per listener), and the rows are
// regrouped into the status document: a status flip restamps, a reason or
// message change holds the sampled time.

// Gateway-level verdicts over the GatewayView listeners list. A Gateway
// with an infrastructure parametersRef is rejected wholesale (no parameter
// kinds are supported); otherwise it is accepted, and programmed, when at
// least one listener is valid.
const anyListenerAccepted = { "@any": [{ "@eq": ["$$.accepted", true] }, "$.listeners"] };
const allListenersAccepted = { "@all": [{ "@eq": ["$$.accepted", true] }, "$.listeners"] };
const gatewayAccepted = {
  "@and": [{ "@eq": ["$.invalidParams", false] }, anyListenerAccepted],
};

// Per-listener verdicts on the flattened row ($.listener is the view's
// listener entry).
const listenerProgrammed = {
  "@and": [{ "@eq": ["$.listener.accepted", true] }, { "@eq": ["$.listener.certOk", true] }],
};
const listenerRefsOk = {
  "@and": [{ "@eq": ["$.listener.certOk", true] }, { "@eq": ["$.listener.kindsResolved", true] }],
};

const status = (boolExpr) => ({ "@cond": [boolExpr, "True", "False"] });

function statusBranches({ addressPool }) {
  return [
    [
      { "@inputs": ["GatewayView"] },
      // One row per listener, carrying the gateway-level verdicts.
      {
        "@project": {
          gateway: "$.gateway",
          generation: "$.generation",
          ...(addressPool ? { address: "$.address" } : {}),
          invalidParams: "$.invalidParams",
          accepted: gatewayAccepted,
          allListenersAccepted: allListenersAccepted,
          listener: "$.listeners",
        },
      },
      { "@unwind": "$.listener" },
      {
        "@project": [
          { "$.": "$." },
          {
            "$.listener.programmed": listenerProgrammed,
            "$.listener.refsOk": listenerRefsOk,
          },
        ],
      },
      // Accepted and Programmed share one verdict, so they share one
      // sample; the listener conditions are keyed per listener.
      transitionStamp(["$.gateway", "$.accepted"], ["$.acceptedTime", "$.programmedTime"]),
      transitionStamp(["$.gateway", "$.listener.name", "$.listener.accepted"], ["$.listener.acceptedTime"]),
      transitionStamp(["$.gateway", "$.listener.name", "$.listener.programmed"], ["$.listener.programmedTime"]),
      transitionStamp(["$.gateway", "$.listener.name", "$.listener.refsOk"], ["$.listener.resolvedRefsTime"]),
      {
        "@groupBy": [
          {
            gateway: "$.gateway",
            generation: "$.generation",
            ...(addressPool ? { address: "$.address" } : {}),
            invalidParams: "$.invalidParams",
            accepted: "$.accepted",
            allListenersAccepted: "$.allListenersAccepted",
            acceptedTime: "$.acceptedTime",
            programmedTime: "$.programmedTime",
          },
          "$.listener",
          { distinct: true },
        ],
      },
      {
        "@project": {
          apiVersion: `${GATEWAY_API_GROUP}/v1`,
          kind: "Gateway",
          metadata: { name: "$.key.gateway.name", namespace: "$.key.gateway.namespace" },
          status: {
            conditions: [
              {
                type: "Accepted",
                status: status("$.key.accepted"),
                reason: {
                  "@switch": [
                    ["$.key.invalidParams", "InvalidParameters"],
                    ["$.key.allListenersAccepted", "Accepted"],
                    [true, "ListenersNotValid"],
                  ],
                },
                message: {
                  "@switch": [
                    [
                      "$.key.invalidParams",
                      "Unsupported infrastructure parametersRef (no parameter kinds are supported)",
                    ],
                    ["$.key.allListenersAccepted", "Gateway accepted"],
                    ["$.key.accepted", "Some listeners are not valid"],
                    [true, "No valid listeners"],
                  ],
                },
                observedGeneration: "$.key.generation",
                lastTransitionTime: "$.key.acceptedTime",
              },
              {
                type: "Programmed",
                status: status("$.key.accepted"),
                reason: { "@cond": ["$.key.accepted", "Programmed", "Invalid"] },
                message: {
                  "@switch": [
                    ["$.key.invalidParams", "Gateway has an invalid infrastructure parametersRef"],
                    ["$.key.accepted", "Gateway programmed to the xDS data plane"],
                    [true, "No valid listeners to program"],
                  ],
                },
                observedGeneration: "$.key.generation",
                lastTransitionTime: "$.key.programmedTime",
              },
            ],
            // The listener entries carry their spec position (index), so
            // the spec order is a plain stable sort.
            listeners: {
              "@map": [
                {
                  name: "$$.name",
                  supportedKinds: "$$.supportedKinds",
                  attachedRoutes: "$$.attachedRoutes",
                  conditions: [
                    {
                      type: "Accepted",
                      status: status("$$.accepted"),
                      reason: "$$.reason",
                      message: {
                        "@cond": [
                          "$$.accepted",
                          "Listener accepted",
                          "Unsupported protocol (only HTTP and HTTPS are supported)",
                        ],
                      },
                      observedGeneration: "$.key.generation",
                      lastTransitionTime: "$$.acceptedTime",
                    },
                    {
                      type: "Programmed",
                      status: status("$$.programmed"),
                      reason: { "@cond": ["$$.programmed", "Programmed", "Invalid"] },
                      message: {
                        "@cond": [
                          "$$.programmed",
                          "Listener programmed to the xDS data plane",
                          "Listener is not valid",
                        ],
                      },
                      observedGeneration: "$.key.generation",
                      lastTransitionTime: "$$.programmedTime",
                    },
                    {
                      type: "ResolvedRefs",
                      status: status("$$.refsOk"),
                      reason: {
                        "@switch": [
                          [{ "@eq": ["$$.kindsResolved", false] }, "InvalidRouteKinds"],
                          [{ "@eq": ["$$.certOk", false] }, "InvalidCertificateRef"],
                          [true, "ResolvedRefs"],
                        ],
                      },
                      message: {
                        "@switch": [
                          [
                            { "@eq": ["$$.kindsResolved", false] },
                            "The allowedRoutes kinds name kinds this listener protocol cannot admit",
                          ],
                          [
                            { "@eq": ["$$.certOk", false] },
                            "The certificateRef does not resolve to a usable kubernetes.io/tls Secret",
                          ],
                          [true, "Listener references resolved"],
                        ],
                      },
                      observedGeneration: "$.key.generation",
                      lastTransitionTime: "$$.resolvedRefsTime",
                    },
                  ],
                },
                { "@sortByKey": ["$$.index", "$.values"] },
              ],
            },
            // In local address-pool mode every gateway is assigned its own
            // deterministic loopback data-plane address; it is reported
            // here and bound by the Envoy listeners (xds.js).
            ...(addressPool
              ? { addresses: [{ type: "IPAddress", value: "$.key.address" }] }
              : {}),
          },
        },
      },
      { "@output": "GatewayStatus" },
    ],
  ];
}

module.exports = { branches, statusBranches };
