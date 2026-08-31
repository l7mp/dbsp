// HTTPRoute processing, up to the RouteView: (route, parentRef) rows, their
// per-listener admission verdicts, the attachments, the rules with their
// resolved backends, and the per-route view that folds them together.
//
// Routes using unimplemented features are rejected outright - reported as
// Accepted=False/UnsupportedValue and excluded from counting and from the
// data plane - because silently programming them without the requested
// behavior (mirroring dropped, a rewrite ignored) would be worse than
// refusing them.
//
// The status writer (statusBranches, k8s output layer) renders the view
// into HTTPRoute status documents.

const {
  kindInKinds,
  hostnameAdmitted,
  namespaceAllowed,
  sectionMatches,
  effectiveDomains,
  transitionStamp,
} = require("./expr.js");
const { GATEWAY_API_GROUP } = require("../config.js");

// SUPPORTED_HTTP_FILTERS are the HTTPRoute filter types the RDS translation
// implements: the core-conformance set, nothing more. Everything else
// (ResponseHeaderModifier, URLRewrite, RequestMirror, ExtensionRef, ...) is
// rejected at admission, never silently dropped. RequestRedirect is limited
// to hostname and statusCode (scheme/port/path redirects are extended
// features and rejected below).
const SUPPORTED_HTTP_FILTERS = ["RequestHeaderModifier", "RequestRedirect"];

// someRule(pred) is true when some rule of the HTTPRoute document satisfies
// pred ($$ = rule); someOf(pred, listExpr) is the same over a list field of
// the rule ($$ = list element, listExpr evaluated with $$ = rule).
const someOf = (pred, listExpr) => ({
  "@any": [pred, { "@definedOr": [listExpr, []] }],
});
const someRule = (pred) => someOf(pred, "$.spec.rules");

// unsupported detects, on a whole HTTPRoute document, the features we do
// not implement: traffic policies (timeouts, retry, session persistence),
// backendRef-level filters, filter types outside SUPPORTED_HTTP_FILTERS,
// more than one filter of the same type in a rule, scheme/port/path
// redirects, and method, query-parameter or regular-expression matching
// (extended conformance features).
const unsupported = someRule({
  "@or": [
    { "@exists": "$$.timeouts" },
    { "@exists": "$$.retry" },
    { "@exists": "$$.sessionPersistence" },
    someOf({ "@gt": [{ "@len": { "@definedOr": ["$$.filters", []] } }, 0] }, "$$.backendRefs"),
    someOf({ "@not": { "@in": ["$$.type", SUPPORTED_HTTP_FILTERS] } }, "$$.filters"),
    ...SUPPORTED_HTTP_FILTERS.map((type) => ({
      "@gt": [
        { "@len": [{ "@filter": [{ "@eq": ["$$.type", type] }, { "@definedOr": ["$$.filters", []] }] }] },
        1,
      ],
    })),
    someOf(
      {
        "@and": [
          { "@eq": ["$$.type", "RequestRedirect"] },
          {
            "@or": [
              { "@exists": "$$.requestRedirect.scheme" },
              { "@exists": "$$.requestRedirect.port" },
              { "@exists": "$$.requestRedirect.path" },
            ],
          },
        ],
      },
      "$$.filters",
    ),
    someOf(
      {
        "@or": [
          { "@exists": "$$.method" },
          { "@exists": "$$.queryParams" },
          { "@eq": [{ "@definedOr": ["$$.path.type", "PathPrefix"] }, "RegularExpression"] },
          someOf(
            { "@eq": [{ "@definedOr": ["$$.type", "Exact"] }, "RegularExpression"] },
            "$$.headers",
          ),
        ],
      },
      "$$.matches",
    ),
  ],
});

// The route identity, carried through every join and group that keys on
// the route; generation rides inside so observedGeneration reaches the
// status conditions.
const route = {
  name: "$.metadata.name",
  namespace: "$.metadata.namespace",
  generation: { "@definedOr": ["$.metadata.generation", 1] },
};

function branches() {
  return [
    // Namespace labels, for the allowedRoutes.namespaces Selector policy.
    [
      { "@inputs": ["Namespace"] },
      {
        "@project": {
          name: "$.metadata.name",
          labels: { "@definedOr": ["$.metadata.labels", {}] },
        },
      },
      { "@output": "namespaceLabels" },
    ],

    // One row per (route, parentRef); unsupported is a pure function of the
    // route document, carried as a column.
    [
      { "@inputs": ["HTTPRoute"] },
      { "@unwind": "$.spec.parentRefs" },
      {
        "@project": {
          route: route,
          parentRef: "$.spec.parentRefs",
          hostnames: { "@definedOr": ["$.spec.hostnames", []] },
          unsupported: unsupported,
        },
      },
      { "@output": "routeParentRefs" },
    ],

    // The (route, parentRef) rows with their namespace's labels joined on
    // (soft: a namespace document may not have been ingested, in which case
    // Selector policies simply do not match).
    [
      { "@inputs": ["routeParentRefs", "namespaceLabels"] },
      {
        "@join": [
          { "@eq": ["$.routeParentRefs.route.namespace", "$.namespaceLabels.name"] },
          {
            soft: ["namespaceLabels"],
            index: {
              routeParentRefs: { name: "$.route.namespace" },
              namespaceLabels: { name: "$.name" },
            },
          },
        ],
      },
      {
        "@project": [
          { "$.": "$.routeParentRefs" },
          { nsLabels: { "@definedOr": ["$.namespaceLabels.labels", {}] } },
        ],
      },
      { "@output": "routeParents" },
    ],

    // Route x listener verdicts: every (route, parentRef) is checked against
    // every listener of the gateway it names (namespace defaulting to the
    // route's own), and the first failing admission check names the
    // verdict: rejection for an unsupported feature, then the sectionName
    // (if any) must name the listener, the listener must be accepted, its
    // supported kinds must admit HTTPRoute (they default to HTTPRoute and
    // narrow to the valid allowedRoutes.kinds when the listener requests
    // any), the allowedRoutes namespace policy must admit the route, and
    // the hostnames must intersect. "Accepted" is an attachment; the other
    // verdicts feed the route status.
    [
      { "@inputs": ["listeners", "routeParents"] },
      {
        "@join": [
          {
            "@and": [
              { "@eq": ["$.listeners.gateway.name", "$.routeParents.parentRef.name"] },
              {
                "@eq": [
                  "$.listeners.gateway.namespace",
                  {
                    "@definedOr": [
                      "$.routeParents.parentRef.namespace",
                      "$.routeParents.route.namespace",
                    ],
                  },
                ],
              },
            ],
          },
          {
            index: {
              listeners: { name: "$.gateway.name", namespace: "$.gateway.namespace" },
              routeParents: {
                name: "$.parentRef.name",
                namespace: { "@definedOr": ["$.parentRef.namespace", "$.route.namespace"] },
              },
            },
          },
        ],
      },
      {
        "@project": {
          route: "$.routeParents.route",
          parentRef: "$.routeParents.parentRef",
          hostnames: "$.routeParents.hostnames",
          gateway: "$.listeners.gateway",
          invalidParams: "$.listeners.invalidParams",
          listener: {
            name: "$.listeners.listener.name",
            port: "$.listeners.listener.port",
            protocol: "$.listeners.listener.protocol",
            hostname: "$.listeners.listener.hostname",
            rdsName: "$.listeners.listener.rdsName",
          },
          verdict: {
            "@switch": [
              [{ "@eq": ["$.routeParents.unsupported", true] }, "UnsupportedValue"],
              [
                { "@not": sectionMatches("$.routeParents.parentRef", "$.listeners.listener.name") },
                "NoMatchingParent",
              ],
              [{ "@neq": ["$.listeners.listener.accepted", true] }, "NoMatchingParent"],
              [
                { "@not": kindInKinds("HTTPRoute", "$.listeners.listener.supportedKinds") },
                "NotAllowedByListeners",
              ],
              [
                {
                  "@not": namespaceAllowed(
                    "$.routeParents.route.namespace",
                    "$.listeners.listener.fromNamespaces",
                    "$.listeners.gateway.namespace",
                    "$.listeners.listener.nsSelector",
                    "$.routeParents.nsLabels",
                  ),
                },
                "NotAllowedByListeners",
              ],
              [
                {
                  "@not": hostnameAdmitted(
                    "$.routeParents.hostnames",
                    "$.listeners.listener.hostname",
                  ),
                },
                "NoMatchingListenerHostname",
              ],
              [true, "Accepted"],
            ],
          },
        },
      },
      { "@output": "routeListeners" },
    ],

    // Attachments: the accepted (route, listener) pairs, what counts as
    // attached and gets programmed into the data plane, each with the
    // effective (intersected) virtual-host domains. One facet of the
    // routeFacets union (RouteView below); the attachedRoutes counter
    // (gateway.js) selects this facet too.
    [
      { "@inputs": ["routeListeners"] },
      { "@select": { "@eq": ["$.verdict", "Accepted"] } },
      {
        "@project": {
          route: "$.route",
          facet: "attachment",
          attachment: {
            gateway: "$.gateway",
            invalidParams: "$.invalidParams",
            listener: "$.listener",
            domains: effectiveDomains,
          },
        },
      },
      { "@output": "routeFacets" },
    ],

    // Per-(route, parentRef) acceptance: accepted when any listener admits
    // the route, otherwise the most specific failure across the listeners,
    // with reason and message spelled out. The parent facet of routeFacets.
    [
      { "@inputs": ["routeListeners"] },
      {
        "@groupBy": [
          { route: "$.route", parentRef: "$.parentRef" },
          "$.verdict",
          { distinct: true },
        ],
      },
      {
        "@project": {
          route: "$.key.route",
          parentRef: "$.key.parentRef",
          reason: {
            "@switch": [
              ...[
                "UnsupportedValue",
                "Accepted",
                "NoMatchingListenerHostname",
                "NotAllowedByListeners",
              ].map((verdict) => [{ "@in": [verdict, "$.values"] }, verdict]),
              [true, "NoMatchingParent"],
            ],
          },
        },
      },
      {
        "@project": [
          { "$.": "$." },
          {
            accepted: { "@eq": ["$.reason", "Accepted"] },
            message: {
              "@switch": [
                [{ "@eq": ["$.reason", "Accepted"] }, "Route accepted"],
                [
                  { "@eq": ["$.reason", "UnsupportedValue"] },
                  "Route uses unimplemented features and is rejected",
                ],
                [
                  { "@eq": ["$.reason", "NoMatchingListenerHostname"] },
                  "No listener hostname intersects the route hostnames",
                ],
                [
                  { "@eq": ["$.reason", "NotAllowedByListeners"] },
                  "No listener admits this route kind from this namespace",
                ],
                [true, "No listener matched this parent reference"],
              ],
            },
          },
        ],
      },
      {
        "@project": {
          route: "$.route",
          facet: "parent",
          parent: {
            parentRef: "$.parentRef",
            accepted: "$.accepted",
            reason: "$.reason",
            message: "$.message",
          },
        },
      },
      { "@output": "routeFacets" },
    ],

    // One row per (route, rule), the rule digested: matches with the path
    // defaults applied (a rule without matches gets the "/" prefix match),
    // the redirect and header-modifier filters extracted (at most one each
    // by admission; a JSONPath filter yields the bare element on a single
    // match). Rule order is meaningful (match precedence ties break on
    // rule position), so the rules are enumerated before the unwind.
    [
      { "@inputs": ["HTTPRoute"] },
      {
        "@project": {
          route: route,
          rules: { "@enumerate": [{ "@definedOr": ["$.spec.rules", []] }] },
        },
      },
      { "@unwind": "$.rules" },
      {
        "@project": {
          route: "$.route",
          ruleIndex: "$.rules.index",
          matches: {
            "@map": [
              {
                path: {
                  type: { "@definedOr": ["$$.path.type", "PathPrefix"] },
                  value: { "@definedOr": ["$$.path.value", "/"] },
                },
                headers: { "@definedOr": ["$$.headers", []] },
              },
              { "@definedOr": ["$.rules.value.matches", [{}]] },
            ],
          },
          redirect: {
            "@definedOr": ["$.rules.value.filters[?(@.type=='RequestRedirect')].requestRedirect", null],
          },
          headerModifier: {
            "@definedOr": [
              "$.rules.value.filters[?(@.type=='RequestHeaderModifier')].requestHeaderModifier",
              null,
            ],
          },
          backendRefs: { "@definedOr": ["$.rules.value.backendRefs", []] },
        },
      },
      { "@output": "routeRules" },
    ],

    // One row per (route, rule, backendRef), for backend resolution. The
    // cluster name is derived from the backendRef alone.
    [
      { "@inputs": ["routeRules"] },
      { "@unwind": "$.backendRefs" },
      {
        "@project": {
          route: "$.route",
          ruleIndex: "$.ruleIndex",
          clusterName: {
            "@concat": [
              { "@definedOr": ["$.backendRefs.namespace", "$.route.namespace"] },
              "/",
              "$.backendRefs.name",
              "/",
              "$.backendRefs.port",
            ],
          },
          weight: { "@definedOr": ["$.backendRefs.weight", 1] },
          service: {
            name: "$.backendRefs.name",
            namespace: { "@definedOr": ["$.backendRefs.namespace", "$.route.namespace"] },
          },
          port: "$.backendRefs.port",
          group: { "@definedOr": ["$.backendRefs.group", ""] },
          kind: { "@definedOr": ["$.backendRefs.kind", "Service"] },
        },
      },
      { "@output": "ruleBackendRefs" },
    ],

    // Per-rule backend sets, each backendRef checked against the Service
    // ports (kind and group included - only core Services resolve): the ok
    // flag decides between a cluster reference and a direct 500 downstream,
    // and together with invalidKind it feeds the route's ResolvedRefs
    // status.
    [
      { "@inputs": ["ruleBackendRefs", "servicePorts"] },
      {
        "@join": [
          {
            "@and": [
              { "@eq": ["$.ruleBackendRefs.service", "$.servicePorts.service"] },
              { "@eq": ["$.ruleBackendRefs.port", "$.servicePorts.port"] },
              { "@eq": ["$.ruleBackendRefs.group", ""] },
              { "@eq": ["$.ruleBackendRefs.kind", "Service"] },
            ],
          },
          {
            soft: ["servicePorts"],
            // The constant components pin the backendRef's group/kind and
            // index the servicePorts rows under the same constants.
            index: {
              ruleBackendRefs: {
                service: "$.service",
                port: "$.port",
                group: "$.group",
                kind: "$.kind",
              },
              servicePorts: { service: "$.service", port: "$.port", group: "", kind: "Service" },
            },
          },
        ],
      },
      {
        "@groupBy": [
          { route: "$.ruleBackendRefs.route", ruleIndex: "$.ruleBackendRefs.ruleIndex" },
          {
            clusterName: "$.ruleBackendRefs.clusterName",
            weight: "$.ruleBackendRefs.weight",
            service: "$.ruleBackendRefs.service",
            port: "$.ruleBackendRefs.port",
            ok: { "@not": { "@isnil": "$.servicePorts" } },
            invalidKind: {
              "@or": [
                { "@neq": ["$.ruleBackendRefs.group", ""] },
                { "@neq": ["$.ruleBackendRefs.kind", "Service"] },
              ],
            },
          },
        ],
      },
      { "@project": { route: "$.key.route", ruleIndex: "$.key.ruleIndex", backends: "$.values" } },
      { "@output": "ruleBackends" },
    ],

    // Rules with their resolved backend sets (rules without backendRefs -
    // e.g. pure redirects - keep an empty set). The rule facet of
    // routeFacets.
    [
      { "@inputs": ["routeRules", "ruleBackends"] },
      {
        "@join": [
          {
            "@and": [
              { "@eq": ["$.routeRules.route", "$.ruleBackends.route"] },
              { "@eq": ["$.routeRules.ruleIndex", "$.ruleBackends.ruleIndex"] },
            ],
          },
          {
            soft: ["ruleBackends"],
            index: {
              routeRules: { route: "$.route", ruleIndex: "$.ruleIndex" },
              ruleBackends: { route: "$.route", ruleIndex: "$.ruleIndex" },
            },
          },
        ],
      },
      {
        "@project": {
          route: "$.routeRules.route",
          facet: "rule",
          rule: {
            ruleIndex: "$.routeRules.ruleIndex",
            matches: "$.routeRules.matches",
            redirect: "$.routeRules.redirect",
            headerModifier: "$.routeRules.headerModifier",
            backends: { "@definedOr": ["$.ruleBackends.backends", []] },
          },
        },
      },
      { "@output": "routeFacets" },
    ],

    // RouteView: the facet rows of a route regrouped into one row, each
    // list selected by its facet tag. Every route with a parentRef or a
    // rule gets a row (a route with neither has no facets, no status and
    // no data-plane presence, so none is needed).
    [
      { "@inputs": ["routeFacets"] },
      { "@groupBy": ["$.route", null, { distinct: true }] },
      {
        "@project": {
          metadata: { name: "$.key.name", namespace: "$.key.namespace" },
          route: "$.key",
          parents: {
            "@map": ["$$.parent", { "@filter": [{ "@eq": ["$$.facet", "parent"] }, "$.values"] }],
          },
          attachments: {
            "@map": [
              "$$.attachment",
              { "@filter": [{ "@eq": ["$$.facet", "attachment"] }, "$.values"] },
            ],
          },
          rules: {
            "@map": ["$$.rule", { "@filter": [{ "@eq": ["$$.facet", "rule"] }, "$.values"] }],
          },
        },
      },
      { "@output": "RouteView" },
    ],
  ];
}

// ---------------------------------------------------------------------
// HTTPRoute status writer: renders RouteView rows into HTTPRoute status
// documents with one parents[] entry per candidate parentRef. Routes whose
// parentRefs reference no gateway of ours get no status. The view row is
// flattened to one row per parent, the route-level ResolvedRefs and the
// per-parent Accepted transition times are sampled by @stamp keyed on the
// condition's status, and the rows are regrouped into the status document.

// Backend resolution over the RouteView rules: someBackend(pred) is true
// when any backend of any rule satisfies pred ($$ = backend). ResolvedRefs
// is False when a backendRef names a missing Service port; InvalidKind
// wins over BackendNotFound in the reported reason.
const someBackend = (pred) => ({
  "@any": [{ "@any": [pred, "$$.backends"] }, "$.rules"],
});
const backendMissing = someBackend({ "@eq": ["$$.ok", false] });
const backendInvalidKind = someBackend({ "@eq": ["$$.invalidKind", true] });

function statusBranches({ controllerName }) {
  return [
    [
      { "@inputs": ["RouteView"] },
      { "@select": { "@gt": [{ "@len": ["$.parents"] }, 0] } },
      // One row per parent, carrying the route-level backend verdicts.
      {
        "@project": {
          route: "$.route",
          refsOk: { "@not": backendMissing },
          backendInvalidKind: backendInvalidKind,
          parent: "$.parents",
        },
      },
      { "@unwind": "$.parent" },
      transitionStamp(["$.route", "$.refsOk"], ["$.resolvedRefsTime"]),
      transitionStamp(["$.route", "$.parent.parentRef", "$.parent.accepted"], ["$.parent.acceptedTime"]),
      {
        "@groupBy": [
          {
            route: "$.route",
            refsOk: "$.refsOk",
            backendInvalidKind: "$.backendInvalidKind",
            resolvedRefsTime: "$.resolvedRefsTime",
          },
          "$.parent",
          { distinct: true },
        ],
      },
      {
        "@project": {
          apiVersion: `${GATEWAY_API_GROUP}/v1`,
          kind: "HTTPRoute",
          metadata: { name: "$.key.route.name", namespace: "$.key.route.namespace" },
          status: {
            parents: {
              "@map": [
                {
                  parentRef: "$$.parentRef",
                  controllerName: controllerName,
                  conditions: [
                    {
                      type: "Accepted",
                      status: { "@cond": ["$$.accepted", "True", "False"] },
                      reason: "$$.reason",
                      message: "$$.message",
                      observedGeneration: "$.key.route.generation",
                      lastTransitionTime: "$$.acceptedTime",
                    },
                    {
                      type: "ResolvedRefs",
                      status: { "@cond": ["$.key.refsOk", "True", "False"] },
                      reason: {
                        "@switch": [
                          ["$.key.backendInvalidKind", "InvalidKind"],
                          [{ "@not": "$.key.refsOk" }, "BackendNotFound"],
                          [true, "ResolvedRefs"],
                        ],
                      },
                      message: {
                        "@cond": [
                          "$.key.refsOk",
                          "Route references resolved",
                          "One or more backend references could not be resolved",
                        ],
                      },
                      observedGeneration: "$.key.route.generation",
                      lastTransitionTime: "$.key.resolvedRefsTime",
                    },
                  ],
                },
                "$.values",
              ],
            },
          },
        },
      },
      { "@output": "HTTPRouteStatus" },
    ],
  ];
}

module.exports = { branches, statusBranches };
