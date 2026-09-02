// E5 ablation: the delta-gateway controller hosted IN-PROCESS on the
// embedded API server (real watch/patch machinery, real delta-ADS data
// plane, no etcd). Comparing these numbers against bench.js on the shared
// envtest plant prices the real Kubernetes API path (apiserver + etcd
// round trips)  -  the plant is the only difference. This is also where the
// xDS fan-out and reconnect-storm scenarios live: they exercise the xDS
// server side, where the API plant is not in the measured path.
//
// Usage:
//   dbsp ablation.js --trace=multiregion|sock-shop --regions=R
//                    --mode=reconciler|open|sotw|smith [--reps=10] [--seed=42]
//                    [--clients=1] [--storm] [--stagger-ms=0]
//                    [--out=results] [--api-port=18443] [--xds-port=18100]
//                    [--timeout-ms=60000]
//
// Default run: converge the trace world, then measure the shape's
// changesets. --clients=C attaches C delta-ADS clients and reports
// time-to-last-client percentiles per delta. --storm skips changesets:
// after convergence it attaches all C clients at once (or staggered) and
// measures time until every client holds the full world.

const minimist = require("minimist");
const fs = require("fs");
const { stats, csvAppend } = require("../lib/measure.js");
const dgw = require("../lib/dgwstack.js");
const traces = require("../lib/traces.js");

const argv = minimist(process.argv.slice(2), {
  boolean: ["storm"],
  string: ["trace", "mode", "out"],
  default: {
    trace: "multiregion",
    regions: 10,
    mode: "reconciler",
    reps: 10,
    seed: 42,
    clients: 1,
    storm: false,
    "stagger-ms": 0,
    out: "results",
    "api-port": 18443,
    "xds-port": 18100,
    "timeout-ms": 60000,
  },
});
const cfg = {
  // "stunner-udp" is the legacy alias of the multiregion shape.
  trace: ["stunner-udp", "multiregion-l4"].includes(argv.trace) ? "multiregion" : argv.trace,
  regions: Number(argv.regions),
  mode: argv.mode,
  reps: Number(argv.reps),
  seed: Number(argv.seed),
  clients: Number(argv.clients),
  storm: !!argv.storm,
  staggerMs: Number(argv["stagger-ms"]),
  out: argv.out,
  timeoutMs: Number(argv["timeout-ms"]),
};

const stack = new dgw.Stack({
  mode: cfg.mode,
  apiPort: Number(argv["api-port"]),
  xdsPort: Number(argv["xds-port"]),
});

const world = traces.generate(cfg.trace, cfg.regions, cfg.seed);
const routeKind = world.routes[0].kind;
const homeNs = world.namespaces[0].metadata.name;
const homeSlices = world.slices.filter((s) => s.metadata.namespace === homeNs);

if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}

// --- Extra xDS clients (fan-out / storm). Client 0 is the stack's own. ---
// Each extra client subscribes to all four resource types like a real
// Envoy; per-client taps record the arrival of markers.
const clientTaps = [];
function addClient(i) {
  const taps = {};
  for (const type of ["lds", "rds", "cds", "eds"]) {
    const topic = `bench.xds.c${i}.${type}`;
    xds.watch(topic, { type: type, address: stack.xdsAddr, node: `bench-envoy-${i}` });
    taps[type] = new dgw.Tap(topic, stack);
  }
  clientTaps.push(taps);
}

// --- Preload. ---
async function preload() {
  const t0 = performance.now();
  stack.write("Namespace", world.namespaces);
  stack.write("GatewayClass", world.gatewayClasses);
  stack.write("Gateway", world.gateways);
  stack.write("Service", world.services);
  stack.write("EndpointSlice", world.slices);
  stack.write(routeKind, world.routes);
  await stack.waitUntil(
    () => stack.taps.lds.events >= world.gateways.length && stack.taps.eds.events >= world.services.length,
    cfg.timeoutMs * 10,
    "preload xDS convergence",
  );
  await stack.settle(1500, cfg.timeoutMs * 10);
  // Convergence ends at the last observed activity: the settle window is
  // synchronization, not workload, and must not ride in the measurement.
  return stack.lastActivity - t0;
}

// --- Changesets (shape-specific). Each returns {xdsMs, statusMs|NaN}. ---

// Wait helpers over one tap and all extra client taps: returns
// [t_first_client0, t_last_all_clients].
async function awaitAll(type, pred, label) {
  const waits = [stack.taps[type].waitMatch(pred, cfg.timeoutMs, label)];
  for (const c of clientTaps) {
    waits.push(c[type].waitMatch(pred, cfg.timeoutMs, `${label} (client)`));
  }
  const ats = await Promise.all(waits);
  return [ats[0], Math.max(...ats)];
}

function regionDocs(i) {
  const ns = `region-${i}`;
  const svc = `turn-svc-${i}`;
  const gwName = `gw-${i}`;
  return {
    ns: ns,
    marker: gwName,
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
  stack.write("Namespace", [r.nsDoc]);
  stack.write("Service", [r.svcDoc]);
  stack.write("EndpointSlice", [r.sliceDoc]);
  await stack.settle(150, cfg.timeoutMs);

  const t0 = performance.now();
  stack.write("Gateway", [r.gw]);
  stack.write("HTTPRoute", [r.route]);
  const [[xdsFirst, xdsLast], statusAt] = await Promise.all([
    awaitAll("lds", (d, w) => w > 0 && JSON.stringify(d).includes(r.marker), `lds ${r.marker}`),
    stack.taps.gateway.waitMatch(
      (d, w) => w > 0 && d?.metadata?.name === r.marker && JSON.stringify(d.status || {}).includes('"Programmed"'),
      cfg.timeoutMs,
      `status ${r.marker}`,
    ),
  ]);
  // Teardown (unmeasured).
  stack.write("HTTPRoute", [r.route], -1);
  stack.write("Gateway", [r.gw], -1);
  await stack.settle(200, cfg.timeoutMs);
  return { xdsMs: xdsFirst - t0, xdsLastMs: xdsLast - t0, statusMs: statusAt - t0 };
}

async function regionDelete(j) {
  // Set up an extra region, converge, then measure its removal.
  const r = regionDocs(200000 + j);
  stack.write("Namespace", [r.nsDoc]);
  stack.write("Service", [r.svcDoc]);
  stack.write("EndpointSlice", [r.sliceDoc]);
  stack.write("Gateway", [r.gw]);
  stack.write("HTTPRoute", [r.route]);
  await stack.taps.lds.waitMatch((d, w) => w > 0 && JSON.stringify(d).includes(r.marker), cfg.timeoutMs, `setup ${r.marker}`);
  await stack.settle(200, cfg.timeoutMs);

  const t0 = performance.now();
  stack.write("HTTPRoute", [r.route], -1);
  stack.write("Gateway", [r.gw], -1);
  const [xdsFirst, xdsLast] = await awaitAll(
    "lds",
    (d, w) => w < 0 && JSON.stringify(d).includes(r.marker),
    `lds retract ${r.marker}`,
  );
  await stack.settle(200, cfg.timeoutMs);
  return { xdsMs: xdsFirst - t0, xdsLastMs: xdsLast - t0, statusMs: NaN };
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
  const [xdsFirst, xdsLast] = await awaitAll(
    "eds",
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    `eds ${marker}`,
  );
  // Restore (unmeasured).
  stack.write("EndpointSlice", [base]);
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsFirst - t0, xdsLastMs: xdsLast - t0, statusMs: NaN };
}

// endpoint-add: grow a home-region slice by one address  -  a pod scale-up.
async function endpointAdd(j) {
  const base = homeSlices[j % homeSlices.length];
  const marker = `10.97.${(j / 250) | 0}.${1 + (j % 250)}`;
  const grown = JSON.parse(JSON.stringify(base));
  grown.endpoints.push({ addresses: [marker], conditions: { ready: true } });
  const t0 = performance.now();
  stack.write("EndpointSlice", [grown]);
  const [xdsFirst, xdsLast] = await awaitAll(
    "eds",
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    `eds add ${marker}`,
  );
  // Restore (unmeasured).
  stack.write("EndpointSlice", [base]);
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsFirst - t0, xdsLastMs: xdsLast - t0, statusMs: NaN };
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
  await stack.taps.eds.waitMatch(
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    cfg.timeoutMs,
    `setup ${marker}`,
  );
  await stack.settle(150, cfg.timeoutMs);
  const t0 = performance.now();
  stack.write("EndpointSlice", [base]);
  const [xdsFirst, xdsLast] = await awaitAll(
    "eds",
    (d, w) => w < 0 && JSON.stringify(d).includes(marker),
    `eds del ${marker}`,
  );
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsFirst - t0, xdsLastMs: xdsLast - t0, statusMs: NaN };
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
  const [[xdsFirst, xdsLast], statusAt] = await Promise.all([
    awaitAll("rds", (d, w) => w > 0 && JSON.stringify(d).includes(host), `rds ${host}`),
    stack.taps.route.waitMatch(
      (d, w) => w > 0 && d?.metadata?.name === name && JSON.stringify(d.status || {}).includes('"Accepted"'),
      cfg.timeoutMs,
      `status ${name}`,
    ),
  ]);
  stack.write("HTTPRoute", [route], -1);
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsFirst - t0, xdsLastMs: xdsLast - t0, statusMs: statusAt - t0 };
}

async function routeUpdate(j) {
  const orig = world.routes.find((r) => r.metadata.namespace === homeNs && r.metadata.name === "catalogue-route");
  const route = JSON.parse(JSON.stringify(orig));
  const marker = `/bench-upd-${j}`;
  route.spec.rules[0].matches = [{ path: { type: "PathPrefix", value: marker } }];
  const t0 = performance.now();
  stack.write("HTTPRoute", [route]);
  const [xdsFirst, xdsLast] = await awaitAll(
    "rds",
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    `rds ${marker}`,
  );
  stack.write("HTTPRoute", [orig]);
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsFirst - t0, xdsLastMs: xdsLast - t0, statusMs: NaN };
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
  await stack.taps.rds.waitMatch((d, w) => w > 0 && JSON.stringify(d).includes(host), cfg.timeoutMs, `setup ${host}`);
  await stack.settle(150, cfg.timeoutMs);
  const t0 = performance.now();
  stack.write("HTTPRoute", [route], -1);
  const [xdsFirst, xdsLast] = await awaitAll(
    "rds",
    (d, w) => w < 0 && JSON.stringify(d).includes(host),
    `rds retract ${host}`,
  );
  await stack.settle(150, cfg.timeoutMs);
  return { xdsMs: xdsFirst - t0, xdsLastMs: xdsLast - t0, statusMs: NaN };
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

// --- Storm mode: attach all clients after convergence. ---
async function runStorm() {
  const preloadMs = await preload();
  const expected = {
    lds: world.gateways.length,
    cds: world.services.length,
    eds: world.services.length,
  };
  const t0 = performance.now();
  for (let i = 1; i <= cfg.clients; i++) {
    addClient(i);
    if (cfg.staggerMs > 0) {
      await dgw.sleep(cfg.staggerMs);
    }
  }
  await stack.waitUntil(
    () =>
      clientTaps.every(
        (c) => c.lds.events >= expected.lds && c.eds.events >= expected.eds && c.cds.events >= expected.cds,
      ),
    cfg.timeoutMs * 10,
    "storm sync",
  );
  const syncMs = performance.now() - t0;
  const bytes = clientTaps.reduce((a, c) => a + c.lds.bytes + c.rds.bytes + c.cds.bytes + c.eds.bytes, 0);
  csvAppend(
    `${cfg.out}/E5-storm-${cfg.trace}.csv`,
    ["clients", "stagger_ms", "regions", "sync_ms", "mbytes", "preload_ms", "runtime_errors"],
    [cfg.clients, cfg.staggerMs, cfg.regions, syncMs.toFixed(0), (bytes / 1e6).toFixed(1), preloadMs.toFixed(0), stack.runtimeErrors],
  );
  console.log(
    `E5 storm trace=${cfg.trace} clients=${cfg.clients} stagger=${cfg.staggerMs}ms: ` +
      `sync=${syncMs.toFixed(0)}ms bytes=${(bytes / 1e6).toFixed(1)}MB errors=${stack.runtimeErrors}`,
  );
}

// --- Default mode: changesets (with optional extra clients attached). ---
async function runChangesets() {
  for (let i = 1; i < cfg.clients; i++) {
    addClient(i);
  }
  const preloadMs = await preload();
  console.log(
    `preload: trace=${cfg.trace} regions=${cfg.regions} mode=${cfg.mode} in ${preloadMs.toFixed(0)}ms ` +
      `(lds=${stack.taps.lds.events} rds=${stack.taps.rds.events} eds=${stack.taps.eds.events})`,
  );

  for (const [name, fn] of Object.entries(CHANGESETS[cfg.trace])) {
    const xdsLat = [];
    const xdsLastLat = [];
    const statusLat = [];
    await fn(cfg.reps + 1); // warmup
    for (let j = 1; j <= cfg.reps; j++) {
      const r = await fn(j);
      xdsLat.push(r.xdsMs);
      xdsLastLat.push(r.xdsLastMs);
      if (!Number.isNaN(r.statusMs)) {
        statusLat.push(r.statusMs);
      }
    }
    const xs = stats(xdsLat);
    const xl = stats(xdsLastLat);
    const ss = stats(statusLat.length > 0 ? statusLat : [NaN]);
    csvAppend(
      `${cfg.out}/E5-ablation-${cfg.trace}-${cfg.mode}-${name}${cfg.clients > 1 ? `-c${cfg.clients}` : ""}.csv`,
      [
        "regions", "clients", "reps",
        "xds_median_ms", "xds_p95_ms", "xds_last_median_ms", "xds_last_p95_ms",
        "status_median_ms", "status_p95_ms", "preload_ms", "runtime_errors",
      ],
      [
        cfg.regions, cfg.clients, cfg.reps,
        xs.median.toFixed(3), xs.p95.toFixed(3), xl.median.toFixed(3), xl.p95.toFixed(3),
        statusLat.length > 0 ? ss.median.toFixed(3) : "",
        statusLat.length > 0 ? ss.p95.toFixed(3) : "",
        preloadMs.toFixed(0), stack.runtimeErrors,
      ],
    );
    console.log(
      `E5 ablation ${cfg.trace} ${cfg.mode} r=${cfg.regions} c=${cfg.clients} ${name}: ` +
        `xds median=${xs.median.toFixed(2)}ms p95=${xs.p95.toFixed(2)}ms` +
        (cfg.clients > 1 ? ` last=${xl.median.toFixed(2)}ms` : "") +
        (statusLat.length > 0 ? ` status=${ss.median.toFixed(2)}ms` : ""),
    );
  }
}

if (cfg.storm) {
  await runStorm();
} else {
  await runChangesets();
}
console.log(`E5 ablation done: trace=${cfg.trace} mode=${cfg.mode} regions=${cfg.regions} runtime_errors=${stack.runtimeErrors}`);
console.log(`STATS ${JSON.stringify(runtime.stats())}`);
exit();
