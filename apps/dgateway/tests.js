// Self-contained test suite for the delta-gateway operator.
//
// The pipeline is driven entirely through topics: Kubernetes fixtures are
// published to the input topics, status outputs are asserted on the status
// topics, and the xDS outputs are verified end-to-end by watching our own
// in-process xDS server back into verification topics (a delta ADS client
// loop, exercising the same path Envoy would).
//
// The cases share one world: the web gateway with its route and the three
// backend Services stay live from the second case on, and every case that
// adds objects retracts them again.
//
// Run from the repo root:  ./js/bin/dbsp apps/dgateway/index.js test

const { describe, assert } = require("testing");
const { TOPICS, CONTROLLER_NAME, OPERATOR } = require("./lib/config.js");
const { compilePipeline } = require("./lib/pipeline.js");
const {
  use,
  collector,
  expectEqual,
  expectCondition,
  byName,
  upsert,
  retract,
  replace,
} = require("./lib/testlib.js");
const fixtures = require("./lib/fixtures.js");

// Condition transition times are @stamp samples of the round clock.
const isTimestamp = (s) => /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(s);
const timeOf = (conditions, type) => conditions.find((cc) => cc.type === type).lastTransitionTime;
// The web gateway's sampled times, held across the cases that edit the
// gateway without flipping a condition.
const held = {};

function setup() {
  // The xDS egress server carries the operator's name; the loader binds
  // the xDS targets to it. Everything else is topic-driven: the watched
  // resources are published by the cases and the statuses read back from
  // the shared status topics.
  const server = xds.server.start({ name: OPERATOR, address: "127.0.0.1:0" });
  use(compilePipeline({}).handle, (t) => !t.startsWith("verify."));

  // Watch our own xDS server back into verification topics.
  xds.watch("verify.lds", { type: "lds", address: server.address });
  xds.watch("verify.rds", { type: "rds", address: server.address });
  xds.watch("verify.cds", { type: "cds", address: server.address });
  xds.watch("verify.eds", { type: "eds", address: server.address });

  return {
    gwcStatus: collector(TOPICS.status.gatewayClass),
    gwStatus: collector(TOPICS.status.gateway),
    routeStatus: collector(TOPICS.status.httpRoute),
    lds: collector("verify.lds"),
    rds: collector("verify.rds"),
    cds: collector("verify.cds"),
    eds: collector("verify.eds"),
  };
}

const c = setup();

describe("delta-gateway", (it) => {
  it("accepts owned GatewayClass, ignores foreign one", async () => {
    upsert(TOPICS.inputs.gatewayClass, fixtures.gatewayClass());
    upsert(TOPICS.inputs.gatewayClass, fixtures.foreignGatewayClass());

    const docs = await c.gwcStatus.waitFor((d) => d.length === 1, "one status");
    assert.strictEqual(docs[0].metadata.name, "delta-gateway");
    expectCondition(docs[0].status.conditions, "Accepted", "True", "Accepted", "gwc");
  });

  it("programs the full HTTP chain: statuses, LDS, RDS, CDS and EDS", async () => {
    upsert(TOPICS.inputs.gateway, fixtures.gateway());
    upsert(TOPICS.inputs.route, fixtures.httpRoute());
    upsert(TOPICS.inputs.service, fixtures.webService("login-svc"));
    upsert(TOPICS.inputs.service, fixtures.webService("api-svc-v1"));
    upsert(TOPICS.inputs.service, fixtures.webService("api-svc-v2"));
    upsert(TOPICS.inputs.endpointSlice, fixtures.webEndpointSlice("login-svc", ["10.0.3.1"]));
    upsert(TOPICS.inputs.endpointSlice, fixtures.webEndpointSlice("api-svc-v1", ["10.0.3.2"]));
    upsert(TOPICS.inputs.endpointSlice, fixtures.webEndpointSlice("api-svc-v2", ["10.0.3.3"]));

    // Gateway status: accepted, one listener with one attached route.
    const gws = await c.gwStatus.waitForQuiet(
      (d) =>
        d.length === 1 &&
        d[0].status.listeners.length === 1 &&
        d[0].status.listeners[0].attachedRoutes === 1,
      "gateway accepted with attached route",
    );
    const gw = gws[0];
    expectCondition(gw.status.conditions, "Accepted", "True", "Accepted", "gw");
    expectCondition(gw.status.conditions, "Programmed", "True", "Programmed", "gw");
    assert.ok(gw.status.conditions.every((cc) => isTimestamp(cc.lastTransitionTime)), "gateway condition times");
    held.accepted = timeOf(gw.status.conditions, "Accepted");
    held.programmed = timeOf(gw.status.conditions, "Programmed");
    const ls = gw.status.listeners[0];
    assert.ok(ls.conditions.every((cc) => isTimestamp(cc.lastTransitionTime)), "listener condition times");
    held.listenerAccepted = timeOf(ls.conditions, "Accepted");
    expectEqual(
      ls.supportedKinds,
      [{ group: "gateway.networking.k8s.io", kind: "HTTPRoute" }],
      "supportedKinds",
    );
    expectCondition(ls.conditions, "Accepted", "True", "Accepted", "listener");
    expectCondition(ls.conditions, "Programmed", "True", "Programmed", "listener");
    expectCondition(ls.conditions, "ResolvedRefs", "True", "ResolvedRefs", "listener");

    // HTTPRoute status: one accepted parent with resolved references.
    const rss = await c.routeStatus.waitForQuiet((d) => d.length === 1, "route status");
    assert.strictEqual(rss[0].kind, "HTTPRoute");
    assert.strictEqual(rss[0].apiVersion, "gateway.networking.k8s.io/v1");
    const parents = rss[0].status.parents;
    assert.strictEqual(parents.length, 1);
    assert.strictEqual(parents[0].controllerName, CONTROLLER_NAME);
    expectEqual(parents[0].parentRef, { name: "web", sectionName: "web" }, "parentRef");
    expectCondition(parents[0].conditions, "Accepted", "True", "Accepted", "route");
    expectCondition(parents[0].conditions, "ResolvedRefs", "True", "ResolvedRefs", "refs");
    assert.ok(parents[0].conditions.every((cc) => isTimestamp(cc.lastTransitionTime)), "route condition times");

    // LDS: an HTTP connection manager pulling routes over RDS.
    const lds = await c.lds.waitForQuiet((d) => d.length === 1, "one xDS listener");
    assert.strictEqual(lds[0].name, "default/web/8080");
    assert.strictEqual(lds[0].address.socketAddress.portValue, 8080);
    const hcm = lds[0].filterChains[0].filters[0].typedConfig;
    assert.strictEqual(hcm.rds.routeConfigName, "default/web/8080");

    // RDS: one virtual host for the route hostname, routes in precedence
    // order (exact before prefix), weighted clusters on the API route.
    const rds = await c.rds.waitForQuiet(
      (d) => d.length === 1 && (d[0].virtualHosts || []).length === 1,
      "route configuration",
    );
    assert.strictEqual(rds[0].name, "default/web/8080");
    const vh = rds[0].virtualHosts[0];
    expectEqual(vh.domains, ["www.example.com"], "vhost domains");
    assert.strictEqual(vh.routes.length, 2);
    assert.strictEqual(vh.routes[0].match.path, "/login");
    assert.strictEqual(vh.routes[0].route.cluster, "default/login-svc/80");
    assert.strictEqual(vh.routes[1].match.pathSeparatedPrefix, "/api");
    expectEqual(
      vh.routes[1].route.weightedClusters.clusters,
      [
        { name: "default/api-svc-v1/80", weight: 90 },
        { name: "default/api-svc-v2/80", weight: 10 },
      ],
      "weighted clusters",
    );

    // CDS/EDS: one cluster per backend, endpoints resolved.
    const cds = await c.cds.waitForQuiet((d) => d.length === 3, "three clusters");
    assert.ok(cds.every((cl) => cl.type === "EDS"), "EDS clusters");
    const eds = await c.eds.waitForQuiet((d) => d.length === 3, "three load assignments");
    const login = eds.find((e) => e.clusterName === "default/login-svc/80");
    expectEqual(
      login.endpoints[0].lbEndpoints.map((e) => e.endpoint.address.socketAddress),
      [{ address: "10.0.3.1", portValue: 8080 }],
      "login endpoints",
    );
  });

  it("marks unsupported protocol listeners invalid, keeps gateway accepted", async () => {
    replace(TOPICS.inputs.gateway, fixtures.gateway(), fixtures.gatewayWithUnsupportedListener());

    const gws = await c.gwStatus.waitForQuiet(
      (d) => d.length === 1 && d[0].status.listeners.length === 2,
      "two listener statuses",
    );
    const gw = gws[0];
    expectCondition(gw.status.conditions, "Accepted", "True", "ListenersNotValid", "gw");
    expectCondition(gw.status.conditions, "Programmed", "True", "Programmed", "gw");
    // The Accepted reason changed but its status did not: the sampled
    // transition times are held.
    assert.strictEqual(timeOf(gw.status.conditions, "Accepted"), held.accepted, "Accepted time held");
    assert.strictEqual(timeOf(gw.status.conditions, "Programmed"), held.programmed, "Programmed time held");

    const [sctp, web] = byName(gw.status.listeners);
    assert.strictEqual(web.name, "web");
    assert.strictEqual(web.attachedRoutes, 1);
    expectCondition(web.conditions, "Accepted", "True", "Accepted", "web listener");
    assert.strictEqual(timeOf(web.conditions, "Accepted"), held.listenerAccepted, "web listener time held");
    assert.ok(sctp.conditions.every((cc) => isTimestamp(cc.lastTransitionTime)), "sctp condition times");
    assert.strictEqual(sctp.name, "sctp");
    assert.strictEqual(sctp.attachedRoutes, 0);
    expectCondition(sctp.conditions, "Accepted", "False", "UnsupportedProtocol", "sctp listener");
    expectCondition(sctp.conditions, "Programmed", "False", "Invalid", "sctp listener");
    expectEqual(sctp.supportedKinds, [], "sctp supportedKinds");

    // The invalid listener must not be programmed into xDS.
    const lds = await c.lds.waitForQuiet((d) => d.length === 1, "still one xDS listener");
    assert.strictEqual(lds[0].name, "default/web/8080");

    // Restore the single-listener gateway for the remaining cases.
    replace(TOPICS.inputs.gateway, fixtures.gatewayWithUnsupportedListener(), fixtures.gateway());
    await c.gwStatus.waitForQuiet(
      (d) => d.length === 1 && d[0].status.listeners.length === 1,
      "back to one listener",
    );
  });

  it("rejects allowedRoutes kinds the listener cannot admit", async () => {
    // A listener requesting a route kind HTTP cannot carry ends up with no
    // supported kinds (ResolvedRefs=False/InvalidRouteKinds) and attaches
    // nothing: a route targeting it is NotAllowedByListeners.
    const kindsGw = fixtures.gateway({
      metadata: { name: "kinds", namespace: "default" },
      spec: {
        gatewayClassName: "delta-gateway",
        listeners: [
          {
            name: "web",
            protocol: "HTTP",
            port: 8082,
            allowedRoutes: { kinds: [{ kind: "TCPRoute" }] },
          },
        ],
      },
    });
    const route = fixtures.httpRoute({
      metadata: { name: "kinds-route", namespace: "default" },
      spec: {
        parentRefs: [{ name: "kinds", sectionName: "web" }],
        rules: [{ backendRefs: [{ name: "login-svc", port: 80 }] }],
      },
    });
    upsert(TOPICS.inputs.gateway, kindsGw);
    upsert(TOPICS.inputs.route, route);

    const gws = await c.gwStatus.waitForQuiet((d) => {
      const kinds = d.find((g) => g.metadata.name === "kinds");
      return kinds && kinds.status.listeners.length === 1;
    }, "kinds gateway status");
    const ls = gws.find((g) => g.metadata.name === "kinds").status.listeners[0];
    expectEqual(ls.supportedKinds, [], "kinds supportedKinds");
    assert.strictEqual(ls.attachedRoutes, 0);
    expectCondition(ls.conditions, "ResolvedRefs", "False", "InvalidRouteKinds", "kinds refs");

    const rss = await c.routeStatus.waitForQuiet(
      (d) => d.some((r) => r.metadata.name === "kinds-route"),
      "kinds route status",
    );
    expectCondition(
      rss.find((r) => r.metadata.name === "kinds-route").status.parents[0].conditions,
      "Accepted",
      "False",
      "NotAllowedByListeners",
      "kinds route",
    );

    retract(TOPICS.inputs.route, route);
    retract(TOPICS.inputs.gateway, kindsGw);
    await c.routeStatus.waitForQuiet((d) => d.length === 1, "kinds route status retracted");
    await c.gwStatus.waitForQuiet((d) => d.length === 1, "kinds gateway status retracted");
  });

  it("reports NoMatchingParent for routes with a non-matching sectionName", async () => {
    const stray = fixtures.httpRoute({
      metadata: { name: "stray-route", namespace: "default" },
      spec: {
        parentRefs: [{ name: "web", sectionName: "no-such-listener" }],
        rules: [{ backendRefs: [{ name: "login-svc", port: 80 }] }],
      },
    });
    upsert(TOPICS.inputs.route, stray);

    // attachedRoutes stays 1; the stray route gets a NoMatchingParent parent
    // entry (its parentRef names our gateway, but no listener matches).
    await c.gwStatus.waitForQuiet(
      (d) => d.length === 1 && d[0].status.listeners[0].attachedRoutes === 1,
      "attachedRoutes unchanged",
    );
    const rss = await c.routeStatus.waitForQuiet(
      (d) => d.some((r) => r.metadata.name === "stray-route"),
      "stray route status",
    );
    const strayStatus = rss.find((r) => r.metadata.name === "stray-route");
    assert.strictEqual(strayStatus.status.parents.length, 1);
    expectCondition(
      strayStatus.status.parents[0].conditions,
      "Accepted",
      "False",
      "NoMatchingParent",
      "stray parent",
    );
    expectCondition(
      strayStatus.status.parents[0].conditions,
      "ResolvedRefs",
      "True",
      "ResolvedRefs",
      "stray refs",
    );

    retract(TOPICS.inputs.route, stray);
    await c.routeStatus.waitForQuiet((d) => d.length === 1, "stray status retracted");
  });

  it("reports unresolved backend references", async () => {
    // A dedicated gateway/route pair whose backendRef names a missing
    // Service: the route attaches, but ResolvedRefs goes False.
    const probeGw = fixtures.gateway({
      metadata: { name: "probe", namespace: "default" },
      spec: {
        gatewayClassName: "delta-gateway",
        listeners: [{ name: "h1", protocol: "HTTP", port: 8099 }],
      },
    });
    const orphan = fixtures.httpRoute({
      metadata: { name: "orphan-route", namespace: "default" },
      spec: {
        parentRefs: [{ name: "probe", sectionName: "h1" }],
        rules: [{ backendRefs: [{ name: "missing-svc", port: 80 }] }],
      },
    });
    upsert(TOPICS.inputs.gateway, probeGw);
    upsert(TOPICS.inputs.route, orphan);

    const rss = await c.routeStatus.waitForQuiet(
      (d) => d.some((r) => r.metadata.name === "orphan-route"),
      "orphan route status",
    );
    const orphanStatus = rss.find((r) => r.metadata.name === "orphan-route");
    expectCondition(
      orphanStatus.status.parents[0].conditions,
      "Accepted",
      "True",
      "Accepted",
      "orphan parent",
    );
    expectCondition(
      orphanStatus.status.parents[0].conditions,
      "ResolvedRefs",
      "False",
      "BackendNotFound",
      "orphan refs",
    );

    // The listener is programmed and its route answers 500 (the HTTPRoute
    // rule has no resolvable backend); no cluster or load assignment
    // exists for the missing backend.
    await c.lds.waitForQuiet((d) => d.length === 2, "probe listener programmed");
    const rds = await c.rds.waitForQuiet((d) => {
      const cfg = d.find((r) => r.name === "default/probe/8099");
      return cfg && (cfg.virtualHosts || []).length === 1;
    }, "probe route configuration");
    const probeRoute = rds.find((r) => r.name === "default/probe/8099").virtualHosts[0].routes[0];
    assert.strictEqual(probeRoute.match.prefix, "/");
    assert.strictEqual(probeRoute.directResponse.status, 500);
    await c.cds.waitForQuiet((d) => d.length === 3, "no cluster for missing backend");
    await c.eds.waitForQuiet((d) => d.length === 3, "no assignment for missing backend");

    retract(TOPICS.inputs.route, orphan);
    retract(TOPICS.inputs.gateway, probeGw);
    await c.lds.waitForQuiet((d) => d.length === 1, "probe listener retracted");
    await c.routeStatus.waitForQuiet((d) => d.length === 1, "orphan status retracted");
    await c.gwStatus.waitForQuiet((d) => d.length === 1, "probe status retracted");
  });

  it("tracks endpoint changes incrementally", async () => {
    replace(
      TOPICS.inputs.endpointSlice,
      fixtures.webEndpointSlice("login-svc", ["10.0.3.1"]),
      fixtures.webEndpointSlice("login-svc", ["10.0.3.1", "10.0.3.4"]),
    );
    await c.eds.waitForQuiet((d) => {
      const login = d.find((e) => e.clusterName === "default/login-svc/80");
      return login && login.endpoints[0].lbEndpoints.length === 2;
    }, "second endpoint added");

    // Removing the slice entirely retracts the load assignment.
    retract(TOPICS.inputs.endpointSlice, fixtures.webEndpointSlice("login-svc", ["10.0.3.1", "10.0.3.4"]));
    await c.eds.waitForQuiet(
      (d) => d.length === 2 && !d.some((e) => e.clusterName === "default/login-svc/80"),
      "load assignment retracted",
    );

    upsert(TOPICS.inputs.endpointSlice, fixtures.webEndpointSlice("login-svc", ["10.0.3.1"]));
    await c.eds.waitForQuiet((d) => {
      const login = d.find((e) => e.clusterName === "default/login-svc/80");
      return d.length === 3 && login && login.endpoints[0].lbEndpoints.length === 1;
    }, "endpoint back");
  });

  it("retracts route status and xDS state on route deletion", async () => {
    retract(TOPICS.inputs.route, fixtures.httpRoute());

    await c.routeStatus.waitForQuiet((d) => d.length === 0, "route status gone");
    await c.gwStatus.waitForQuiet(
      (d) => d.length === 1 && d[0].status.listeners[0].attachedRoutes === 0,
      "attachedRoutes back to zero",
    );
    // The listener stays (a routeless HTTP listener serves 404s through an
    // empty route configuration); clusters and endpoints go.
    await c.lds.waitForQuiet((d) => d.length === 1, "xDS listener kept");
    await c.rds.waitForQuiet((d) => d.length === 1 && !d[0].virtualHosts, "empty route config");
    await c.cds.waitForQuiet((d) => d.length === 0, "xDS clusters gone");
    await c.eds.waitForQuiet((d) => d.length === 0, "load assignments gone");

    // Re-creating the route restores the data plane for the remaining cases.
    upsert(TOPICS.inputs.route, fixtures.httpRoute());
    await c.rds.waitForQuiet(
      (d) => d.length === 1 && (d[0].virtualHosts || []).length === 1,
      "route config back",
    );
    await c.cds.waitForQuiet((d) => d.length === 3, "clusters back");
    await c.eds.waitForQuiet((d) => d.length === 3, "load assignments back");
  });

  it("enforces listener hostname intersection for HTTP routes", async () => {
    upsert(TOPICS.inputs.gateway, fixtures.hostGateway());

    // A routeless HTTP listener still serves an (empty) route configuration.
    await c.rds.waitForQuiet(
      (d) => d.some((r) => r.name === "default/host/8081" && !r.virtualHosts),
      "empty route configuration",
    );

    // api.example.com intersects the *.example.com listener; other.org does
    // not and must be rejected without touching the data plane.
    const matching = fixtures.httpRoute({
      metadata: { name: "api-route", namespace: "default" },
      spec: {
        parentRefs: [{ name: "host", sectionName: "web" }],
        hostnames: ["api.example.com", "other.org"],
        rules: [{ backendRefs: [{ name: "login-svc", port: 80 }] }],
      },
    });
    const foreign = fixtures.httpRoute({
      metadata: { name: "foreign-route", namespace: "default" },
      spec: {
        parentRefs: [{ name: "host", sectionName: "web" }],
        hostnames: ["other.org"],
        rules: [{ backendRefs: [{ name: "login-svc", port: 80 }] }],
      },
    });
    upsert(TOPICS.inputs.route, matching);
    upsert(TOPICS.inputs.route, foreign);

    // The matching route attaches and its virtual host carries only the
    // intersected domain; a default "/" prefix match is synthesized for the
    // match-less rule.
    const rds = await c.rds.waitForQuiet((d) => {
      const cfg = d.find((r) => r.name === "default/host/8081");
      return cfg && (cfg.virtualHosts || []).length === 1;
    }, "intersected vhost");
    const cfg = rds.find((r) => r.name === "default/host/8081");
    expectEqual(cfg.virtualHosts[0].domains, ["api.example.com"], "intersected domains");
    assert.strictEqual(cfg.virtualHosts[0].routes[0].match.prefix, "/");

    const rss = await c.routeStatus.waitForQuiet(
      (d) => d.some((r) => r.metadata.name === "foreign-route"),
      "foreign route status",
    );
    expectCondition(
      rss.find((r) => r.metadata.name === "api-route").status.parents[0].conditions,
      "Accepted",
      "True",
      "Accepted",
      "api route",
    );
    expectCondition(
      rss.find((r) => r.metadata.name === "foreign-route").status.parents[0].conditions,
      "Accepted",
      "False",
      "NoMatchingListenerHostname",
      "foreign route",
    );

    retract(TOPICS.inputs.route, matching);
    retract(TOPICS.inputs.route, foreign);
    retract(TOPICS.inputs.gateway, fixtures.hostGateway());
    await c.rds.waitForQuiet((d) => d.length === 1, "host route configuration retracted");
  });

  it("rejects HTTPRoutes that use filters with UnsupportedValue", async () => {
    upsert(TOPICS.inputs.route, fixtures.mirrorRoute());

    const rss = await c.routeStatus.waitForQuiet(
      (d) => d.some((r) => r.metadata.name === "mirror-route"),
      "mirror route status",
    );
    const mirror = rss.find((r) => r.metadata.name === "mirror-route");
    expectCondition(
      mirror.status.parents[0].conditions,
      "Accepted",
      "False",
      "UnsupportedValue",
      "mirror route",
    );

    // The rejected route is not counted and not programmed: the web listener
    // still has one attached route and the RDS config still has one vhost
    // with the two original routes.
    await c.gwStatus.waitForQuiet(
      (d) => d.length === 1 && d[0].status.listeners[0].attachedRoutes === 1,
      "attachedRoutes unchanged by rejected route",
    );
    const rds = await c.rds.waitForQuiet(
      (d) => d.length === 1 && (d[0].virtualHosts || []).length === 1,
      "route configuration unchanged",
    );
    assert.strictEqual(rds[0].virtualHosts[0].routes.length, 2);

    retract(TOPICS.inputs.route, fixtures.mirrorRoute());
    await c.routeStatus.waitForQuiet(
      (d) => !d.some((r) => r.metadata.name === "mirror-route"),
      "mirror status retracted",
    );
  });

  it("programs HTTPRoute filters: redirect and request header modifier", async () => {
    upsert(TOPICS.inputs.route, fixtures.filteredRoute());

    // The route is accepted (its filter types are all implemented) and its
    // two rules land in the shared www.example.com virtual host in match
    // precedence order: /login (exact), then /old, /svc, /api (equal-length
    // prefixes tie-break on the route name).
    const rss = await c.routeStatus.waitForQuiet(
      (d) => d.some((r) => r.metadata.name === "filtered-route"),
      "filtered route status",
    );
    expectCondition(
      rss.find((r) => r.metadata.name === "filtered-route").status.parents[0].conditions,
      "Accepted",
      "True",
      "Accepted",
      "filtered route",
    );

    const rds = await c.rds.waitForQuiet(
      (d) =>
        d.length === 1 &&
        (d[0].virtualHosts || []).length === 1 &&
        d[0].virtualHosts[0].routes.length === 4,
      "filtered routes programmed",
    );
    const routes = rds[0].virtualHosts[0].routes;

    // RequestRedirect: hostname redirect with an explicit 301, the original
    // listener port preserved (Envoy's host_redirect alone would drop it).
    // 301 maps to MOVED_PERMANENTLY, Envoy's proto default, which protojson
    // elides; its absence IS the 301.
    assert.strictEqual(routes[1].match.pathSeparatedPrefix, "/old");
    assert.strictEqual(routes[1].redirect.hostRedirect, "new.example.com");
    assert.strictEqual(routes[1].redirect.portRedirect, 8080);
    assert.strictEqual(routes[1].redirect.responseCode, undefined);

    // The request header modifier rides on the forwarding action.
    assert.strictEqual(routes[2].match.pathSeparatedPrefix, "/svc");
    assert.strictEqual(routes[2].route.cluster, "default/login-svc/80");
    expectEqual(
      routes[2].requestHeadersToAdd,
      [
        {
          header: { key: "x-env", value: "prod" },
          appendAction: "OVERWRITE_IF_EXISTS_OR_ADD",
        },
      ],
      "request header set",
    );
    expectEqual(routes[2].requestHeadersToRemove, ["x-debug"], "request header remove");

    retract(TOPICS.inputs.route, fixtures.filteredRoute());
    await c.rds.waitForQuiet(
      (d) => d.length === 1 && d[0].virtualHosts[0].routes.length === 2,
      "filtered routes retracted",
    );
  });

  it("terminates TLS on HTTPS listeners once the certificate resolves", async () => {
    // Without the Secret the listener is accepted but not programmed, and
    // ResolvedRefs reports the invalid certificateRef.
    upsert(TOPICS.inputs.gateway, fixtures.secureGateway());

    const gws = await c.gwStatus.waitForQuiet((d) => {
      const secure = d.find((g) => g.metadata.name === "secure");
      return secure && secure.status.listeners.length === 1;
    }, "secure gateway status");
    let https = gws.find((g) => g.metadata.name === "secure").status.listeners[0];
    expectEqual(
      https.supportedKinds,
      [{ group: "gateway.networking.k8s.io", kind: "HTTPRoute" }],
      "https supportedKinds",
    );
    expectCondition(https.conditions, "Accepted", "True", "Accepted", "https listener");
    expectCondition(
      https.conditions,
      "ResolvedRefs",
      "False",
      "InvalidCertificateRef",
      "https refs",
    );
    expectCondition(https.conditions, "Programmed", "False", "Invalid", "https listener");
    await c.lds.waitForQuiet(
      (d) => !d.some((l) => l.name === "default/secure/8443"),
      "unresolved listener not programmed",
    );

    // The certificate Secret resolves the listener: programmed with a TLS
    // transport socket carrying the inline (base64) payloads.
    upsert(TOPICS.inputs.secret, fixtures.tlsSecret());

    const lds = await c.lds.waitForQuiet(
      (d) => d.some((l) => l.name === "default/secure/8443"),
      "https listener programmed",
    );
    const httpsL = lds.find((l) => l.name === "default/secure/8443");
    assert.strictEqual(httpsL.address.socketAddress.portValue, 8443);
    const hcm = httpsL.filterChains[0].filters[0].typedConfig;
    assert.strictEqual(hcm.rds.routeConfigName, "default/secure/https");
    const tls = httpsL.filterChains[0].transportSocket.typedConfig;
    expectEqual(
      tls.commonTlsContext.tlsCertificates[0],
      {
        certificateChain: {
          inlineBytes:
            "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCmNlcnQtY2hhaW4tcGVtCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0=",
        },
        privateKey: {
          inlineBytes:
            "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCnByaXZhdGUta2V5LXBlbQotLS0tLUVORCBQUklWQVRFIEtFWS0tLS0t",
        },
      },
      "tls certificate payload",
    );

    const gws2 = await c.gwStatus.waitForQuiet((d) => {
      const secure = d.find((g) => g.metadata.name === "secure");
      const l = secure && secure.status.listeners[0];
      return l && l.conditions.some((cc) => cc.type === "Programmed" && cc.status === "True");
    }, "https listener programmed status");
    https = gws2.find((g) => g.metadata.name === "secure").status.listeners[0];
    expectCondition(https.conditions, "ResolvedRefs", "True", "ResolvedRefs", "https refs ok");

    // HTTPRoutes attach to HTTPS listeners.
    const secureRoute = fixtures.httpRoute({
      metadata: { name: "secure-route", namespace: "default" },
      spec: {
        parentRefs: [{ name: "secure", sectionName: "https" }],
        hostnames: ["www.example.com"],
        rules: [{ backendRefs: [{ name: "login-svc", port: 80 }] }],
      },
    });
    upsert(TOPICS.inputs.route, secureRoute);

    const rds = await c.rds.waitForQuiet((d) => {
      const cfg = d.find((r) => r.name === "default/secure/https");
      return cfg && (cfg.virtualHosts || []).length === 1;
    }, "https route configuration");
    const cfg = rds.find((r) => r.name === "default/secure/https");
    expectEqual(cfg.virtualHosts[0].domains, ["www.example.com"], "https vhost domains");

    retract(TOPICS.inputs.route, secureRoute);
    retract(TOPICS.inputs.gateway, fixtures.secureGateway());
    retract(TOPICS.inputs.secret, fixtures.tlsSecret());
    await c.lds.waitForQuiet(
      (d) => !d.some((l) => l.name === "default/secure/8443"),
      "https listener retracted",
    );
  });

  it("applies BackendTLSPolicy to clusters, suppressing unhonorable ones", async () => {
    // The web chain is live: three plain clusters.
    await c.cds.waitForQuiet((d) => d.length === 3, "three plain clusters");

    // A System-CA policy adds an upstream TLS transport socket with SNI.
    upsert(TOPICS.inputs.backendTLSPolicy, fixtures.backendTLSPolicy("login-svc"));
    const cds = await c.cds.waitForQuiet(
      (d) => d.some((cl) => cl.name === "default/login-svc/80" && cl.transportSocket),
      "tls cluster",
    );
    const login = cds.find((cl) => cl.name === "default/login-svc/80");
    const tls = login.transportSocket.typedConfig;
    assert.strictEqual(tls.sni, "login-svc.example.internal");
    assert.strictEqual(
      tls.commonTlsContext.validationContext.trustedCa.filename,
      "/etc/ssl/certs/ca-certificates.crt",
    );

    // A caCertificateRefs policy cannot be honored: the cluster and its load
    // assignment are suppressed rather than programmed plaintext.
    upsert(TOPICS.inputs.backendTLSPolicy, fixtures.unsupportedBackendTLSPolicy("api-svc-v1"));
    await c.cds.waitForQuiet(
      (d) => d.length === 2 && !d.some((cl) => cl.name === "default/api-svc-v1/80"),
      "unhonorable cluster suppressed",
    );
    await c.eds.waitForQuiet(
      (d) => d.length === 2 && !d.some((e) => e.clusterName === "default/api-svc-v1/80"),
      "unhonorable assignment suppressed",
    );

    retract(TOPICS.inputs.backendTLSPolicy, fixtures.backendTLSPolicy("login-svc"));
    retract(TOPICS.inputs.backendTLSPolicy, fixtures.unsupportedBackendTLSPolicy("api-svc-v1"));
    await c.cds.waitForQuiet(
      (d) => d.length === 3 && d.every((cl) => !cl.transportSocket),
      "plain clusters restored",
    );
  });

  it("keeps invalid-params gateways out of the data plane", async () => {
    const invalid = fixtures.gateway({
      metadata: { name: "misconfigured", namespace: "default" },
      spec: {
        gatewayClassName: "delta-gateway",
        listeners: [{ name: "http", protocol: "HTTP", port: 8083 }],
        infrastructure: { parametersRef: { group: "x", kind: "Params", name: "nope" } },
      },
    });
    upsert(TOPICS.inputs.gateway, invalid);

    // Status: rejected with InvalidParameters.
    const gws = await c.gwStatus.waitFor(
      (d) => d.some((g) => g.metadata.name === "misconfigured"),
      "invalid gateway status",
    );
    const gw = gws.find((g) => g.metadata.name === "misconfigured");
    expectCondition(gw.status.conditions, "Accepted", "False", "InvalidParameters", "invalid gw");

    // Data plane: no listener is programmed for it.
    await c.lds.waitForQuiet(
      (d) => !d.some((l) => l.name === "default/misconfigured/8083"),
      "invalid gateway not programmed",
    );

    retract(TOPICS.inputs.gateway, invalid);
    await c.gwStatus.waitForQuiet(
      (d) => !d.some((g) => g.metadata.name === "misconfigured"),
      "invalid gateway retracted",
    );
    await c.cds.waitForQuiet(
      (d) => d.length === 3 && d.every((cl) => !cl.transportSocket),
      "baseline intact",
    );
  });
}).then(
  () => exit(0),
  () => exit(1),
);
