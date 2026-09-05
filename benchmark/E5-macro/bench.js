// E5: the macrobenchmark  -  trace-shaped worlds, per-changeset convergence
// and cold start, every contender out of process against the shared
// envtest plant (kube-apiserver + etcd). The driver is the same for every
// system: it writes the world through the plant kubeconfig, watches
// statuses back from the plant, and measures the data plane with the
// connector's delta-ADS clients attached to the contender's xDS server
// (plaintext wildcard for delta-gateway, mTLS per-gateway for
// envoy-gateway). The contender itself is spawned by run.sh: the
// delta-gateway controller (reconciler / open / sotw) or envoy-gateway in
// serve mode.
//
// Usage:
//   dbsp bench.js --trace=multiregion|sock-shop --regions=R
//                 --system=dgw|eg [--mode=reconciler|open|sotw|smith] [--reps=10]
//                 [--seed=42] [--handshake=/tmp/gw-plant/handshake.json]
//                 [--eg-handshake=/tmp/eg-serve/handshake.json]
//                 [--xds-address=127.0.0.1:19000] [--out=results]
//                 [--timeout-ms=60000]

const minimist = require("minimist");
const fs = require("fs");
const { stats, csvAppend } = require("../lib/measure.js");
const { K8sStack } = require("../lib/k8sstack.js");
const dgw = require("../lib/dgwstack.js");
const traces = require("../lib/traces.js");

const argv = minimist(process.argv.slice(2), {
  string: ["trace", "system", "mode", "variant", "out", "handshake", "eg-handshake", "xds-address"],
  default: {
    trace: "multiregion",
    regions: 10,
    system: "dgw",
    mode: "reconciler",
    reps: 10,
    seed: 42,
    handshake: "/tmp/gw-plant/handshake.json",
    "eg-handshake": "/tmp/eg-serve/handshake.json",
    "xds-address": "",
    out: "results",
    "timeout-ms": 60000,
  },
});
const cfg = {
  // "stunner-udp" is the legacy alias of the multiregion shape.
  trace: ["stunner-udp", "multiregion-l4"].includes(argv.trace) ? "multiregion" : argv.trace,
  regions: Number(argv.regions),
  system: argv.system,
  reps: Number(argv.reps),
  seed: Number(argv.seed),
  out: argv.out,
  timeoutMs: Number(argv["timeout-ms"]),
};
// The architecture label: the dgw modes are what run.sh started the
// controller with; envoy-gateway and istio are their own lines. A variant
// suffix (e.g. the smith window, "k250") makes its own line and CSVs.
const mode = (cfg.system === "dgw" ? argv.mode : cfg.system) + (argv.variant ? `-${argv.variant}` : "");

const world = traces.generate(cfg.trace, cfg.regions, cfg.seed);
const routeKind = world.routes[0].kind;
const homeNs = world.namespaces[0].metadata.name; // changeset target region

// The envoy-gateway and istio measurement clients subscribe per gateway:
// the home region (endpoint and route changesets) plus every gateway the
// region changesets will create. delta-gateway needs none of this  -  its
// wildcard client sees everything.
const watchGateways = [[homeNs, "gw-1"]];
for (let j = 1; j <= cfg.reps + 1; j++) {
  watchGateways.push([`region-${100000 + j}`, `gw-${100000 + j}`]);
  watchGateways.push([`region-${200000 + j}`, `gw-${200000 + j}`]);
}

// The endpoint changesets rotate over the home region's slices so every
// system observes them (the per-gateway taps only see subscribed
// gateways).
const homeSlices = world.slices.filter((s) => s.metadata.namespace === homeNs);

// istio serves Endpoint and Route resources strictly by name: subscribe
// to the home region's clusters and the gateway's route configuration.
const xdsResources = {
  eds: homeSlices.map((s) => {
    const svc = s.metadata.labels["kubernetes.io/service-name"];
    const port = s.ports[0].port;
    return `outbound|${port}||${svc}.${homeNs}.svc.cluster.local`;
  }),
  rds: [`http.${world.gateways[0].spec.listeners[0].port}`],
};

const stack = new K8sStack({
  system: cfg.system,
  handshake: argv.handshake,
  egHandshake: argv["eg-handshake"],
  xdsAddress: argv["xds-address"],
  watchGateways: watchGateways,
  xdsResources: xdsResources,
});
const taps = stack.taps;

if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}

function hasCondition(doc, type, status) {
  const conds =
    doc?.status?.conditions ||
    (doc?.status?.parents || []).flatMap((p) => p.conditions || []);
  return conds.some((c) => c.type === type && c.status === status);
}

// --- Preload (the cold-start measurement). ---
async function preload() {
  const t0 = performance.now();
  await stack.ensureNamespaces(world.namespaces, cfg.timeoutMs * 10);
  stack.write("GatewayClass", world.gatewayClasses);
  stack.write("Gateway", world.gateways);
  stack.write("Service", world.services);
  stack.write("EndpointSlice", world.slices);
  stack.write(routeKind, world.routes);
  if (cfg.system !== "dgw") {
    // The xDS taps only cover the subscribed gateways; converge on the
    // route statuses instead.
    await stack.waitUntil(
      () => stack.acceptedRoutes("") >= world.routes.length,
      cfg.timeoutMs * 10,
      "preload status convergence",
    );
  } else {
    await stack.waitUntil(
      () => taps.lds.events >= world.gateways.length && taps.eds.events >= world.services.length,
      cfg.timeoutMs * 10,
      "preload xDS convergence",
    );
  }
  await stack.settle(1500, cfg.timeoutMs * 10);
  // Convergence ends at the last observed activity: the settle window is
  // synchronization, not workload, and must not ride in the measurement.
  return stack.lastActivity - t0;
}

// --- Changesets (shape-specific). Each returns {xdsMs, statusMs|NaN}. ---

function regionDocs(i) {
  const ns = `region-${i}`;
  const svc = `turn-svc-${i}`;
  const gwName = `gw-${i}`;
  return {
    ns: ns,
    // The delivery marker: dgw and envoy-gateway carry the gateway name in
    // the listener (LDS); istio's HTTP route names the backend service in
    // its cluster reference, which lands in RDS.
    marker: cfg.system === "istio" ? svc : gwName,
    nsDoc: { kind: "Namespace", metadata: { name: ns } },
    svcDoc: {
      kind: "Service",
      metadata: { name: svc, namespace: ns },
      spec: { ports: [{ name: "app", protocol: "TCP", port: 3478, targetPort: 3478 }] },
    },
    sliceDoc: {
      kind: "EndpointSlice",
      metadata: { name: `${svc}-abc12`, namespace: ns, labels: { "kubernetes.io/service-name": svc } },
      addressType: "IPv4",
      endpoints: [{ addresses: [`10.99.${(i / 250) % 250 | 0}.${1 + (i % 250)}`], conditions: { ready: true } }],
      ports: [{ name: "app", protocol: "TCP", port: 3478 }],
    },
    gw: dgw.fixtures.gateway({
      metadata: { name: gwName, namespace: ns },
      spec: { gatewayClassName: "delta-gateway", listeners: [{ name: "turn", protocol: "HTTP", port: 3478 }] },
    }),
    route: {
      apiVersion: "gateway.networking.k8s.io/v1",
      kind: "HTTPRoute",
      metadata: { name: `turn-route-${i}`, namespace: ns },
      spec: { parentRefs: [{ name: gwName, sectionName: "turn" }], rules: [{ backendRefs: [{ name: svc, port: 3478 }] }] },
    },
  };
}

// multiregion: a region is the deployment unit  -  add/remove whole
// tuples; endpoint churn is the steady-state event.
async function regionAdd(j) {
  const r = regionDocs(100000 + j);
  const mtap = cfg.system === "istio" ? taps.rds : taps.lds;
  await stack.ensureNamespaces([r.nsDoc], cfg.timeoutMs);
  stack.write("Service", [r.svcDoc]);
  stack.write("EndpointSlice", [r.sliceDoc]);
  await stack.settle(150, cfg.timeoutMs);

  const t0 = performance.now();
  stack.write("Gateway", [r.gw]);
  stack.write("HTTPRoute", [r.route]);
  const [xdsAt, statusAt] = await Promise.all([
    mtap.waitMatch((d, w) => w > 0 && JSON.stringify(d).includes(r.marker), cfg.timeoutMs, `lds ${r.marker}`),
    taps.gateway.waitMatch(
      // Without provisioned infra envoy-gateway leaves Programmed=False
      // (AddressNotAssigned); Accepted is the comparable gate there.
      // istio's manual-mode gateways reach Programmed=True (the companion
      // Service resolves the address), like dgw.
      (d, w) =>
        w > 0 &&
        d?.metadata?.name === `gw-${100000 + j}` &&
        hasCondition(d, cfg.system === "eg" ? "Accepted" : "Programmed", "True"),
      cfg.timeoutMs,
      `status gw-${100000 + j}`,
    ),
  ]);
  // Teardown (unmeasured).
  stack.write("HTTPRoute", [r.route], -1);
  stack.write("Gateway", [r.gw], -1);
  await stack.settle(200, cfg.timeoutMs);
  return { xdsMs: xdsAt - t0, statusMs: statusAt - t0 };
}

async function regionDelete(j) {
  // Set up an extra region, converge, then measure its removal.
  const r = regionDocs(200000 + j);
  const mtap = cfg.system === "istio" ? taps.rds : taps.lds;
  await stack.ensureNamespaces([r.nsDoc], cfg.timeoutMs);
  stack.write("Service", [r.svcDoc]);
  stack.write("EndpointSlice", [r.sliceDoc]);
  stack.write("Gateway", [r.gw]);
  stack.write("HTTPRoute", [r.route]);
  await mtap.waitMatch((d, w) => w > 0 && JSON.stringify(d).includes(r.marker), cfg.timeoutMs, `setup ${r.marker}`);
  await stack.settle(200, cfg.timeoutMs);

  const t0 = performance.now();
  stack.write("HTTPRoute", [r.route], -1);
  stack.write("Gateway", [r.gw], -1);
  const xdsAt = await mtap.waitMatch(
    (d, w) => w < 0 && JSON.stringify(d).includes(r.marker),
    cfg.timeoutMs,
    `lds retract ${r.marker}`,
  );
  await stack.settle(200, cfg.timeoutMs);
  return { xdsMs: xdsAt - t0, statusMs: NaN };
}

// endpoint-update: replace one endpoint address in a home-region slice  - 
// the steady-state churn event in both shapes.
async function endpointUpdate(j) {
  const base = homeSlices[j % homeSlices.length];
  const slice = JSON.parse(JSON.stringify(base));
  const marker = `10.98.${(j / 250) | 0}.${1 + (j % 250)}`;
  slice.endpoints[0].addresses = [marker];
  const t0 = performance.now();
  stack.write("EndpointSlice", [slice]);
  const xdsAt = await taps.eds.waitMatch(
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    cfg.timeoutMs,
    `eds ${marker}`,
  );
  // Restore (unmeasured).
  stack.write("EndpointSlice", [base]);
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsAt - t0, statusMs: NaN };
}

// endpoint-add: grow a home-region slice by one address  -  a pod scale-up.
async function endpointAdd(j) {
  const base = homeSlices[j % homeSlices.length];
  const marker = `10.97.${(j / 250) | 0}.${1 + (j % 250)}`;
  const grown = JSON.parse(JSON.stringify(base));
  grown.endpoints.push({ addresses: [marker], conditions: { ready: true } });
  const t0 = performance.now();
  stack.write("EndpointSlice", [grown]);
  const xdsAt = await taps.eds.waitMatch(
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    cfg.timeoutMs,
    `eds add ${marker}`,
  );
  // Restore (unmeasured).
  stack.write("EndpointSlice", [base]);
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsAt - t0, statusMs: NaN };
}

// endpoint-delete: shrink a pre-grown slice back  -  a pod scale-down. The
// growth is unmeasured setup; the measured event is the retraction of the
// assignment still carrying the extra address.
async function endpointDelete(j) {
  const base = homeSlices[j % homeSlices.length];
  const marker = `10.96.${(j / 250) | 0}.${1 + (j % 250)}`;
  const grown = JSON.parse(JSON.stringify(base));
  grown.endpoints.push({ addresses: [marker], conditions: { ready: true } });
  stack.write("EndpointSlice", [grown]);
  await taps.eds.waitMatch(
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    cfg.timeoutMs,
    `setup ${marker}`,
  );
  await stack.settle(150, cfg.timeoutMs);
  const t0 = performance.now();
  stack.write("EndpointSlice", [base]);
  const xdsAt = await taps.eds.waitMatch(
    (d, w) => w < 0 && JSON.stringify(d).includes(marker),
    cfg.timeoutMs,
    `eds del ${marker}`,
  );
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsAt - t0, statusMs: NaN };
}

// sock-shop: route-level changesets on the home region's gateway.
async function routeAdd(j) {
  const name = `bench-route-${j}`;
  const host = `bench-${j}.example.com`;
  const route = dgw.fixtures.httpRoute({
    metadata: { name: name, namespace: homeNs },
    spec: {
      parentRefs: [{ name: "gw-1", sectionName: "web" }],
      hostnames: [host],
      rules: [{ matches: [{ path: { type: "PathPrefix", value: `/bench-${j}` } }], backendRefs: [{ name: "front-end", port: 80 }] }],
    },
  });
  const t0 = performance.now();
  stack.write("HTTPRoute", [route]);
  const [xdsAt, statusAt] = await Promise.all([
    taps.rds.waitMatch((d, w) => w > 0 && JSON.stringify(d).includes(host), cfg.timeoutMs, `rds ${host}`),
    taps.route.waitMatch(
      (d, w) => w > 0 && d?.metadata?.name === name && hasCondition(d, "Accepted", "True"),
      cfg.timeoutMs,
      `status ${name}`,
    ),
  ]);
  stack.write("HTTPRoute", [route], -1);
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsAt - t0, statusMs: statusAt - t0 };
}

async function routeUpdate(j) {
  const orig = world.routes.find((r) => r.metadata.namespace === homeNs && r.metadata.name === "catalogue-route");
  const route = JSON.parse(JSON.stringify(orig));
  const marker = `/bench-upd-${j}`;
  route.spec.rules[0].matches = [{ path: { type: "PathPrefix", value: marker } }];
  const t0 = performance.now();
  stack.write("HTTPRoute", [route]);
  const xdsAt = await taps.rds.waitMatch(
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    cfg.timeoutMs,
    `rds ${marker}`,
  );
  stack.write("HTTPRoute", [orig]);
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsAt - t0, statusMs: NaN };
}

async function routeDelete(j) {
  const name = `bench-del-${j}`;
  const host = `bench-del-${j}.example.com`;
  const route = dgw.fixtures.httpRoute({
    metadata: { name: name, namespace: homeNs },
    spec: {
      parentRefs: [{ name: "gw-1", sectionName: "web" }],
      hostnames: [host],
      rules: [{ matches: [{ path: { type: "PathPrefix", value: `/bench-del-${j}` } }], backendRefs: [{ name: "front-end", port: 80 }] }],
    },
  });
  stack.write("HTTPRoute", [route]);
  await taps.rds.waitMatch((d, w) => w > 0 && JSON.stringify(d).includes(host), cfg.timeoutMs, `setup ${host}`);
  await stack.settle(150, cfg.timeoutMs);
  const t0 = performance.now();
  stack.write("HTTPRoute", [route], -1);
  const xdsAt = await taps.rds.waitMatch(
    (d, w) => w < 0 && JSON.stringify(d).includes(host),
    cfg.timeoutMs,
    `rds retract ${host}`,
  );
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsAt - t0, statusMs: NaN };
}

const CHANGESETS = {
  "multiregion": {
    "region-add": regionAdd,
    "region-delete": regionDelete,
    "endpoint-add": endpointAdd,
    "endpoint-delete": endpointDelete,
    "endpoint-update": endpointUpdate,
  },
  "sock-shop": {
    "route-add": routeAdd,
    "route-update": routeUpdate,
    "route-delete": routeDelete,
    "endpoint-add": endpointAdd,
    "endpoint-delete": endpointDelete,
    "endpoint-update": endpointUpdate,
  },
};

// --- Run. ---
const preloadMs = await preload();
console.log(
  `preload: trace=${cfg.trace} regions=${cfg.regions} mode=${mode} in ${preloadMs.toFixed(0)}ms ` +
    `(lds=${taps.lds.events} rds=${taps.rds.events} eds=${taps.eds.events} status_writes=${stack.statusEvents})`,
);
// Persist the cold-start measurement before the changesets run: a changeset
// timeout aborts the point, and its per-changeset rows (which also carry
// preload_ms) are then never written.
csvAppend(
  `${cfg.out}/E5-preload-raw.csv`,
  ["trace", "mode", "regions", "preload_ms"],
  [cfg.trace, mode, cfg.regions, preloadMs.toFixed(0)],
);

for (const [name, fn] of Object.entries(CHANGESETS[cfg.trace])) {
  const xdsLat = [];
  const statusLat = [];
  await fn(cfg.reps + 1); // warmup
  for (let j = 1; j <= cfg.reps; j++) {
    const r = await fn(j);
    xdsLat.push(r.xdsMs);
    if (!Number.isNaN(r.statusMs)) {
      statusLat.push(r.statusMs);
    }
  }
  const xs = stats(xdsLat);
  const ss = stats(statusLat.length > 0 ? statusLat : [NaN]);
  csvAppend(
    `${cfg.out}/E5-${cfg.trace}-${mode}-${name}.csv`,
    [
      "regions", "reps",
      "xds_median_ms", "xds_p95_ms", "status_median_ms", "status_p95_ms",
      "preload_ms", "runtime_errors",
    ],
    [
      cfg.regions, cfg.reps,
      xs.median.toFixed(3), xs.p95.toFixed(3),
      statusLat.length > 0 ? ss.median.toFixed(3) : "",
      statusLat.length > 0 ? ss.p95.toFixed(3) : "",
      preloadMs.toFixed(0), stack.runtimeErrors,
    ],
  );
  console.log(
    `E5 ${cfg.trace} ${mode} r=${cfg.regions} ${name}: ` +
      `xds median=${xs.median.toFixed(2)}ms p95=${xs.p95.toFixed(2)}ms` +
      (statusLat.length > 0 ? ` status=${ss.median.toFixed(2)}ms` : ""),
  );
}

console.log(`E5 done: trace=${cfg.trace} mode=${mode} regions=${cfg.regions} runtime_errors=${stack.runtimeErrors}`);
console.log(`STATS ${JSON.stringify(runtime.stats())}`);
exit();
