// The xDS output layer: renders the views into Envoy protojson resources.
// Every branch unwinds/regroups view rows and renders - the relational
// admission and resolution work happened in the input layer. Optional
// proto fields are emitted as null when absent: the protojson decode treats
// null as unset. Note "@type" appears as a plain @dict key: only single-key
// maps are parsed as operators.

const { gatewayAddressExpr, sortByKeys } = require("./expr.js");

// Gateway API route match precedence as a sort-key chain (most significant
// first): exact paths beat prefixes, longer prefixes beat shorter ones,
// then more header matches win. Ties break on the route's namespace/name
// (a stand-in for the spec's route creation timestamp, which the rows do
// not carry), the route's own rule order (the first matching rule wins
// within one route), and finally the match content hash for full
// determinism (documents crossing the Go boundary carry no stable key
// order, so a plain serialization could not serve as a stable tie-break).
// Method, query-parameter and regex matching are rejected at admission
// (httproute.js), so they never reach this ranking.
const MATCH_PRECEDENCE_KEYS = [
  { "@cond": [{ "@eq": ["$$.match.path.type", "Exact"] }, 0, 1] },
  {
    "@cond": [
      { "@eq": ["$$.match.path.type", "Exact"] },
      0,
      { "@neg": { "@len": "$$.match.path.value" } },
    ],
  },
  { "@neg": { "@len": "$$.match.headers" } },
  { "@concat": ["$$.route.namespace", "/", "$$.route.name"] },
  "$$.ruleIndex",
  { "@hash": "$$.match" },
];

// Filter-chain order inside an HTTPS listener: canonical, so the rendered
// listener is a deterministic function of the (unordered) Z-set group.
const HTTPS_CHAIN_KEY = { "@concat": ["$$.hostname", "/", "$$.listenerName"] };

// Envoy proto type URLs.
const HCM_TYPE =
  "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager";
const ROUTER_TYPE = "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router";
const DOWNSTREAM_TLS_TYPE =
  "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext";
const TLS_INSPECTOR_TYPE =
  "type.googleapis.com/envoy.extensions.filters.listener.tls_inspector.v3.TlsInspector";
const UPSTREAM_TLS_TYPE =
  "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext";

// SYSTEM_CA_BUNDLE is the CA bundle used for BackendTLSPolicy's
// wellKnownCACertificates: System (the Debian/Ubuntu/Alpine location, which
// covers the standard Envoy images).
const SYSTEM_CA_BUNDLE = "/etc/ssl/certs/ca-certificates.crt";

// The HTTP connection manager of a listener chain: it pulls the route
// configuration named rdsName over RDS/ADS. The stat prefix is the RDS name
// itself (Envoy only requires it to be non-empty).
const httpConnectionManager = (rdsNameExpr) => ({
  name: "envoy.filters.network.http_connection_manager",
  typedConfig: {
    "@type": HCM_TYPE,
    statPrefix: rdsNameExpr,
    rds: { configSource: { ads: {}, resourceApiVersion: "V3" }, routeConfigName: rdsNameExpr },
    // A single-key map is parsed as an operator: @dict keeps "@type" a key.
    httpFilters: [{ name: "envoy.filters.http.router", typedConfig: { "@dict": { "@type": ROUTER_TYPE } } }],
  },
});

// The listener skeleton: the address is the gateway's pool address in
// address-pool mode, the wildcard otherwise.
const listenerAddress = (addressPool, addressExpr, portExpr) => ({
  socketAddress: { address: addressPool ? addressExpr : "0.0.0.0", portValue: portExpr },
});

// A Gateway API path match as an Envoy route match. Gateway API PathPrefix
// is segment-wise, which is Envoy's path_separated_prefix, not the plain
// string prefix; a trailing slash is stripped from it. Exact header matches
// ride along. The three path forms are exclusive: the two that do not apply
// are null (unset).
const isExact = { "@eq": ["$$.match.path.type", "Exact"] };
const isRootPrefix = { "@and": [{ "@not": isExact }, { "@eq": ["$$.match.path.value", "/"] }] };
const isPrefix = { "@and": [{ "@not": isExact }, { "@neq": ["$$.match.path.value", "/"] }] };
const routeMatch = {
  path: { "@cond": [isExact, "$$.match.path.value", null] },
  prefix: { "@cond": [isRootPrefix, "/", null] },
  pathSeparatedPrefix: {
    "@cond": [
      isPrefix,
      {
        "@cond": [
          { "@endswith": ["$$.match.path.value", "/"] },
          { "@substring": ["$$.match.path.value", 1, { "@sub": [{ "@len": "$$.match.path.value" }, 1] }] },
          "$$.match.path.value",
        ],
      },
      null,
    ],
  },
  headers: {
    "@cond": [
      { "@gt": [{ "@len": "$$.match.headers" }, 0] },
      { "@map": [{ name: "$$.name", stringMatch: { exact: "$$.value" } }, "$$.match.headers"] },
      null,
    ],
  },
};

// Header mutations of a RequestHeaderModifier: set overwrites, add appends.
const headerMutation = (action) => ({
  header: { key: "$$.name", value: "$$.value" },
  appendAction: action,
});
const headerMutations = {
  "@append": [
    { "@map": [headerMutation("OVERWRITE_IF_EXISTS_OR_ADD"), { "@definedOr": ["$$.headerModifier.set", []] }] },
    { "@map": [headerMutation("APPEND_IF_EXISTS_OR_ADD"), { "@definedOr": ["$$.headerModifier.add", []] }] },
  ],
};
const nonEmptyOrNull = (listExpr) => ({
  "@cond": [{ "@gt": [{ "@len": listExpr }, 0] }, listExpr, null],
});

// The route action, exactly one of the three: a RequestRedirect replaces
// forwarding (hostname/statusCode only, the redirect keeps the original
// port since Envoy's host_redirect alone would drop it; Gateway API
// defaults the status to 302 while Envoy's proto default is 301, so the
// code is always set); a rule with no usable backend answers 500 per the
// HTTPRoute spec; otherwise the cluster, or the weighted clusters in
// canonical name order.
const hasRedirect = { "@not": { "@isnil": "$$.redirect" } };
const backendCount = { "@len": "$$.backends" };
const routeRedirect = {
  "@cond": [
    hasRedirect,
    {
      hostRedirect: { "@definedOr": ["$$.redirect.hostname", null] },
      portRedirect: "$$.port",
      responseCode: {
        "@cond": [{ "@eq": ["$$.redirect.statusCode", 301] }, "MOVED_PERMANENTLY", "FOUND"],
      },
    },
    null,
  ],
};
const routeDirectResponse = {
  "@cond": [{ "@and": [{ "@not": hasRedirect }, { "@eq": [backendCount, 0] }] }, { status: 500 }, null],
};
const routeAction = {
  "@cond": [
    { "@and": [{ "@not": hasRedirect }, { "@gt": [backendCount, 0] }] },
    {
      "@cond": [
        { "@eq": [backendCount, 1] },
        { cluster: "$$.backends[0].clusterName" },
        {
          weightedClusters: {
            clusters: {
              "@map": [
                { name: "$$.clusterName", weight: "$$.weight" },
                { "@sortByKey": ["$$.clusterName", "$$.backends"] },
              ],
            },
          },
        },
      ],
    },
    null,
  ],
};

// One Envoy route from one entry of a virtual host (the @map subject).
const envoyRoute = {
  match: routeMatch,
  requestHeadersToAdd: nonEmptyOrNull(headerMutations),
  requestHeadersToRemove: nonEmptyOrNull({ "@definedOr": ["$$.headerModifier.remove", []] }),
  redirect: routeRedirect,
  directResponse: routeDirectResponse,
  route: routeAction,
};

function branches({ addressPool } = {}) {
  return [
    // One row per (route attachment, rule): the shared raw material of the
    // route and cluster branches. A gateway rejected for invalid
    // infrastructure parameters is not programmed; its attachments stay
    // visible to the status realm only.
    [
      { "@inputs": ["RouteView"] },
      { "@unwind": "$.attachments" },
      { "@select": { "@not": { "@eq": ["$.attachments.invalidParams", true] } } },
      { "@unwind": "$.rules" },
      {
        "@project": {
          route: "$.route",
          listener: "$.attachments.listener",
          domains: "$.attachments.domains",
          ruleIndex: "$.rules.ruleIndex",
          matches: "$.rules.matches",
          redirect: "$.rules.redirect",
          headerModifier: "$.rules.headerModifier",
          backends: "$.rules.backends",
        },
      },
      { "@output": "attachmentRules" },
    ],

    // Accepted listeners from the GatewayView, one row per listener: the
    // raw material of the data plane. A gateway rejected for invalid
    // infrastructure parameters is not programmed (its status already says
    // Accepted: False, and the data plane must agree); a routeless listener
    // serves 404s via an empty route configuration; HTTPS listeners require
    // a resolved certificate and carry its payload for the transport
    // socket.
    [
      { "@inputs": ["GatewayView"] },
      { "@unwind": "$.listeners" },
      {
        "@select": {
          "@and": [
            { "@not": { "@eq": ["$.invalidParams", true] } },
            { "@eq": ["$.listeners.accepted", true] },
            { "@eq": ["$.listeners.certOk", true] },
          ],
        },
      },
      {
        "@project": {
          gateway: "$.gateway",
          ...(addressPool ? { address: "$.address" } : {}),
          listenerName: "$.listeners.name",
          protocol: "$.listeners.protocol",
          port: "$.listeners.port",
          hostname: "$.listeners.hostname",
          certChain: "$.listeners.certChain",
          privateKey: "$.listeners.privateKey",
          rdsName: "$.listeners.rdsName",
          // Envoy allows one listener per address:port, so the Envoy
          // listener is named per (gateway, port).
          name: { "@concat": ["$.gateway.namespace", "/", "$.gateway.name", "/", "$.listeners.port"] },
        },
      },
      { "@output": "acceptedListeners" },
    ],

    // Envoy listeners for HTTP: the Gateway listeners of one (gateway,
    // port) share a single connection manager pulling the merged per-port
    // route configuration (virtual-host domains implement the per-listener
    // hostnames), so their rows collapse to one by @distinct.
    [
      { "@inputs": ["acceptedListeners"] },
      { "@select": { "@eq": ["$.protocol", "HTTP"] } },
      {
        "@project": {
          name: "$.name",
          port: "$.port",
          ...(addressPool ? { address: "$.address" } : {}),
          rdsName: "$.rdsName",
        },
      },
      "@distinct",
      {
        "@project": {
          name: "$.name",
          address: listenerAddress(addressPool, "$.address", "$.port"),
          filterChains: [{ filters: [httpConnectionManager("$.rdsName")] }],
        },
      },
      { "@output": "XdsListeners" },
    ],

    // Envoy listeners for HTTPS: one SNI-selected filter chain per Gateway
    // listener on the port, each terminating TLS with its own inline
    // (base64) certificate payload and per-listener route configuration.
    // A hostname-less listener is the catch-all chain (no match); hostnames
    // are distinct per port by API-server validation. The TLS inspector
    // extracts the server name from the ClientHello for the chain match.
    [
      { "@inputs": ["acceptedListeners"] },
      { "@select": { "@eq": ["$.protocol", "HTTPS"] } },
      {
        "@groupBy": [
          { name: "$.name", port: "$.port", ...(addressPool ? { address: "$.address" } : {}) },
          {
            listenerName: "$.listenerName",
            hostname: "$.hostname",
            rdsName: "$.rdsName",
            certChain: "$.certChain",
            privateKey: "$.privateKey",
          },
          { distinct: true },
        ],
      },
      {
        "@project": {
          name: "$.key.name",
          address: listenerAddress(addressPool, "$.key.address", "$.key.port"),
          listenerFilters: [
            {
              name: "envoy.filters.listener.tls_inspector",
              typedConfig: { "@dict": { "@type": TLS_INSPECTOR_TYPE } },
            },
          ],
          filterChains: {
            "@map": [
              {
                filterChainMatch: {
                  "@cond": [{ "@neq": ["$$.hostname", ""] }, { serverNames: ["$$.hostname"] }, null],
                },
                filters: [httpConnectionManager("$$.rdsName")],
                transportSocket: {
                  name: "envoy.transport_sockets.tls",
                  typedConfig: {
                    "@type": DOWNSTREAM_TLS_TYPE,
                    commonTlsContext: {
                      tlsCertificates: [
                        {
                          certificateChain: { inlineBytes: "$$.certChain" },
                          privateKey: { inlineBytes: "$$.privateKey" },
                        },
                      ],
                    },
                  },
                },
              },
              sortByKeys([HTTPS_CHAIN_KEY], "$.values"),
            ],
          },
        },
      },
      { "@output": "XdsListeners" },
    ],

    // Virtual hosts: one entry per (attachment, rule, match, domain) - the
    // rule digest (matches defaulted, redirect and headerModifier
    // extracted) comes with the view; only the usable backends (resolved,
    // weighted) are picked here - regrouped per (RDS configuration,
    // domain): grouping on the RDS name merges the entries of same-port
    // HTTP listeners into their shared per-port configuration, HTTPS
    // configurations stay per-listener, and each group renders as one
    // Envoy virtual host with the routes in Gateway API match-precedence
    // order. Envoy picks the most specific matching domain at request
    // time, so wildcard overlap needs no ranking here.
    [
      { "@inputs": ["attachmentRules"] },
      { "@unwind": "$.matches" },
      { "@unwind": "$.domains" },
      {
        "@groupBy": [
          { rdsName: "$.listener.rdsName", domain: "$.domains" },
          {
            route: "$.route",
            ruleIndex: "$.ruleIndex",
            port: "$.listener.port",
            match: "$.matches",
            redirect: "$.redirect",
            headerModifier: "$.headerModifier",
            backends: {
              "@filter": [
                { "@and": [{ "@eq": ["$$.ok", true] }, { "@gt": ["$$.weight", 0] }] },
                "$.backends",
              ],
            },
          },
          { distinct: true },
        ],
      },
      {
        "@project": {
          rdsName: "$.key.rdsName",
          domain: "$.key.domain",
          vhost: {
            name: { "@cond": [{ "@eq": ["$.key.domain", "*"] }, "wildcard", "$.key.domain"] },
            domains: ["$.key.domain"],
            routes: { "@map": [envoyRoute, sortByKeys(MATCH_PRECEDENCE_KEYS, "$.values")] },
          },
        },
      },
      { "@output": "virtualHosts" },
    ],

    // Envoy route configurations, one per RDS name, collecting its virtual
    // hosts in domain order (the soft join keeps routeless listeners, which
    // produce an empty configuration - virtualHosts null is the unset proto
    // field). Hostname matching ignores any explicit port in the Host
    // header; the header itself is forwarded unmodified.
    [
      { "@inputs": ["acceptedListeners", "virtualHosts"] },
      {
        "@join": [
          { "@eq": ["$.acceptedListeners.rdsName", "$.virtualHosts.rdsName"] },
          {
            soft: ["virtualHosts"],
            index: {
              acceptedListeners: { rdsName: "$.rdsName" },
              virtualHosts: { rdsName: "$.rdsName" },
            },
          },
        ],
      },
      {
        "@groupBy": [
          { rdsName: "$.acceptedListeners.rdsName" },
          {
            present: { "@not": { "@isnil": "$.virtualHosts" } },
            domain: { "@definedOr": ["$.virtualHosts.domain", ""] },
            vhost: { "@definedOr": ["$.virtualHosts.vhost", {}] },
          },
          { distinct: true },
        ],
      },
      {
        "@project": {
          name: "$.key.rdsName",
          ignorePortInHostMatching: true,
          virtualHosts: {
            "@cond": [
              { "@any": [{ "@eq": ["$$.present", true] }, "$.values"] },
              {
                "@map": [
                  "$$.vhost",
                  {
                    "@sortByKey": [
                      "$$.domain",
                      { "@filter": [{ "@eq": ["$$.present", true] }, "$.values"] },
                    ],
                  },
                ],
              },
              null,
            ],
          },
        },
      },
      { "@output": "XdsRouteConfigurations" },
    ],

    // Referenced clusters: every resolved backend of every attached route.
    [
      { "@inputs": ["attachmentRules"] },
      { "@unwind": "$.backends" },
      { "@select": { "@eq": ["$.backends.ok", true] } },
      {
        "@project": {
          clusterName: "$.backends.clusterName",
          service: "$.backends.service",
          port: "$.backends.port",
        },
      },
      "@distinct",
      { "@output": "clusterRefs" },
    ],

    // The referenced clusters joined with their BackendView rows. A backend
    // whose TLS policy cannot be honored is suppressed here, which gates
    // both CDS and EDS (fail closed: better a missing cluster than
    // plaintext to a backend that demanded TLS).
    [
      { "@inputs": ["clusterRefs", "BackendView"] },
      {
        "@join": [
          {
            "@and": [
              { "@eq": ["$.clusterRefs.service", "$.BackendView.service"] },
              { "@eq": ["$.clusterRefs.port", "$.BackendView.port"] },
            ],
          },
          {
            index: {
              clusterRefs: { service: "$.service", port: "$.port" },
              BackendView: { service: "$.service", port: "$.port" },
            },
          },
        ],
      },
      { "@select": { "@eq": ["$.BackendView.tlsOk", true] } },
      {
        "@project": {
          clusterName: "$.clusterRefs.clusterName",
          hasTls: "$.BackendView.hasTls",
          sni: "$.BackendView.sni",
          endpoints: "$.BackendView.endpoints",
        },
      },
      { "@output": "clusters" },
    ],

    // Envoy clusters, with the upstream TLS transport socket when a
    // BackendTLSPolicy demands it.
    [
      { "@inputs": ["clusters"] },
      {
        "@project": {
          name: "$.clusterName",
          type: "EDS",
          edsClusterConfig: { edsConfig: { ads: {}, resourceApiVersion: "V3" } },
          transportSocket: {
            "@cond": [
              { "@eq": ["$.hasTls", true] },
              {
                name: "envoy.transport_sockets.tls",
                typedConfig: {
                  "@type": UPSTREAM_TLS_TYPE,
                  sni: "$.sni",
                  commonTlsContext: {
                    validationContext: { trustedCa: { filename: SYSTEM_CA_BUNDLE } },
                  },
                },
              },
              null,
            ],
          },
        },
      },
      { "@output": "XdsClusters" },
    ],

    // Envoy load assignments: the BackendView endpoints of every referenced
    // cluster, in canonical (address, port) order. Clusters with no ready
    // endpoints emit no row (Envoy treats a missing assignment and an empty
    // one the same).
    [
      { "@inputs": ["clusters"] },
      { "@select": { "@gt": [{ "@len": ["$.endpoints"] }, 0] } },
      {
        "@project": {
          clusterName: "$.clusterName",
          endpoints: [
            {
              lbEndpoints: {
                "@map": [
                  {
                    endpoint: {
                      address: { socketAddress: { address: "$$.address", portValue: "$$.port" } },
                    },
                  },
                  sortByKeys(["$$.address", "$$.port"], "$.endpoints"),
                ],
              },
            },
          ],
        },
      },
      { "@output": "XdsEndpoints" },
    ],
  ];
}

module.exports = { branches };
