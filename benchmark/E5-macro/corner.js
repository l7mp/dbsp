// E5 corner cases: the workload shapes where state-of-the-world xDS
// translation genuinely hurts, run against either contender through the
// unified harness on the shared envtest plant (--system=dgw with --mode,
// or --system=eg against the envoy-gateway serve process).
//
//   mega-vhost      ONE gateway, K HTTPRoutes (distinct hostnames, shared
//                   backend). A route-add re-translates and re-serializes
//                   the whole RouteConfiguration under SotW; an
//                   incremental controller emits one vhost delta. Sweep K.
//   endpoint-heavy  M gateways x 1 route, each backend with E endpoints.
//                   A one-address change re-generates every cluster load
//                   assignment under SotW; incremental ships one EDS
//                   delta. Sweep E.
//   saturation      Fixed world; endpoint updates fired at RATE events/s
//                   for DURATION seconds, round-robin across services;
//                   per-update delivery latency and completion counted.
//                   The knee where latency diverges is the control
//                   plane's maximum sustainable churn.
//   amplification   ONE gateway admitting N routes from a label-selected
//                   namespace; an annotation on that namespace  -  a field
//                   no gateway semantics depend on  -  churns at RATE
//                   events/s while an endpoint-update probe measures
//                   interference. Object-granularity dependency tracking
//                   (krt: any change to a fetched object invalidates every
//                   derived item that fetched it) re-computes O(N) items
//                   per churn event; field-granularity algebra kills the
//                   delta at the first projection for free.
//
// Usage:
//   dbsp corner.js --case=mega-vhost --routes=K [--system=... --mode=...]
//   dbsp corner.js --case=endpoint-heavy --endpoints=E [--gateways=10]
//   dbsp corner.js --case=saturation --rate=10 --duration=20 [--gateways=30]

const minimist = require("minimist");
const fs = require("fs");
const { stats, csvAppend } = require("../lib/measure.js");
const { K8sStack } = require("../lib/k8sstack.js");
const dgw = require("../lib/dgwstack.js");

const argv = minimist(process.argv.slice(2), {
  string: ["case", "system", "mode", "variant", "out", "handshake", "eg-handshake", "xds-address", "churn"],
  default: {
    case: "mega-vhost",
    system: "dgw",
    mode: "reconciler",
    handshake: "/tmp/gw-plant/handshake.json",
    "eg-handshake": "/tmp/eg-serve/handshake.json",
    "xds-address": "",
    gateways: 10,
    routes: 100,
    endpoints: 50,
    rate: 10,
    duration: 20,
    reps: 10,
    out: "results",
    "timeout-ms": 120000,
  },
});
const cfg = {
  kase: argv.case,
  system: argv.system,
  m: Number(argv.gateways),
  k: Number(argv.routes),
  e: Number(argv.endpoints),
  rate: Number(argv.rate),
  duration: Number(argv.duration),
  reps: Number(argv.reps),
  out: argv.out,
  timeoutMs: Number(argv["timeout-ms"]),
};
const nonce = Number(argv.nonce || Date.now() % 100000);
const mode = (cfg.system === "dgw" ? argv.mode : cfg.system) + (argv.variant ? `-${argv.variant}` : "");

// Per-case object-name prefixes: each case owns a distinct name family.
const PREFIX = { "mega-vhost": "m", "endpoint-heavy": "h", saturation: "s", amplification: "a" }[cfg.kase];
const gwName = (i) => `${PREFIX}gw-${i}`;
const rtName = (i) => `${PREFIX}rt-${i}`;
const svcName = (i) => `${PREFIX}svc-${i}`;

const watchGateways = (
  cfg.kase === "saturation"
    ? Array.from({ length: cfg.m }, (_, i) => gwName(i + 1))
    : [gwName(1)]
).map((g) => ["default", g]);

// istio serves Endpoint and Route resources strictly by name: subscribe
// to every case service's cluster and the case gateway's route config.
const CASE_NS = cfg.kase === "amplification" ? "amp-routes" : "default";
const xdsResources = {
  eds: Array.from({ length: cfg.m + 1 }, (_, i) =>
    `outbound|80||${svcName(i)}.${CASE_NS}.svc.cluster.local`),
  rds: ["http.8080"],
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
const fixtures = dgw.fixtures;
const write = (kind, docs, w) => stack.write(kind, docs, w);

if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}

function gatewayDoc(name, port) {
  return fixtures.gateway({
    metadata: { name: name, namespace: "default" },
    spec: {
      gatewayClassName: "delta-gateway",
      listeners: [{ name: "web", protocol: "HTTP", port: port }],
    },
  });
}

function routeDoc(name, gw, host, svc) {
  return fixtures.httpRoute({
    metadata: { name: name, namespace: "default" },
    spec: {
      parentRefs: [{ name: gw, sectionName: "web" }],
      hostnames: [host],
      rules: [{ matches: [{ path: { type: "PathPrefix", value: "/" } }], backendRefs: [{ name: svc, port: 80 }] }],
    },
  });
}

function sliceDoc(svc, addrs) {
  return {
    kind: "EndpointSlice",
    metadata: {
      name: `${svc}-eps`,
      namespace: "default",
      labels: { "kubernetes.io/service-name": svc },
    },
    addressType: "IPv4",
    endpoints: addrs.map((a) => ({ addresses: [a], conditions: { ready: true } })),
    ports: [{ name: "http", protocol: "TCP", port: 80 }],
  };
}

function addrs(base, n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    out.push(`10.${base % 200}.${(i / 250) | 0}.${1 + (i % 250)}`);
  }
  return out;
}

async function converge(expectedRoutes) {
  if (cfg.system !== "dgw") {
    await stack.waitUntil(
      () => stack.acceptedRoutes(`${PREFIX}`) >= expectedRoutes,
      cfg.timeoutMs * 5,
      "preload status convergence",
    );
  } else {
    await stack.waitUntil(
      () => taps.rds.events > 0 && taps.eds.events > 0,
      cfg.timeoutMs * 5,
      "preload xDS convergence",
    );
  }
  await stack.settle(1500, cfg.timeoutMs * 5);
}

// --- mega-vhost: 1 gateway, K routes, shared backend. ---
async function megaVhost() {
  write("GatewayClass", [fixtures.gatewayClass()]);
  write("Gateway", [gatewayDoc(gwName(1), 8080)]);
  write("Service", [fixtures.webService(svcName(0))]);
  write("EndpointSlice", [sliceDoc(svcName(0), addrs(1, 4))]);
  const routes = [];
  for (let k = 1; k <= cfg.k; k++) {
    routes.push(routeDoc(rtName(k), gwName(1), `h-${k}.corner.example.com`, svcName(0)));
  }
  const t0 = performance.now();
  write("HTTPRoute", routes);
  await converge(cfg.k);
  const preloadMs = performance.now() - t0;

  const lat = [];
  for (let j = 0; j <= cfg.reps; j++) {
    const host = `add-${nonce}-${j}.corner.example.com`;
    const doc = routeDoc(`${PREFIX}add-${nonce}-${j}`, gwName(1), host, svcName(0));
    const t = performance.now();
    write("HTTPRoute", [doc]);
    const at = await taps.rds.waitMatch(
      (d, w) => w > 0 && JSON.stringify(d).includes(host),
      cfg.timeoutMs,
      `rds ${host}`,
    );
    if (j > 0) {
      lat.push(at - t); // j=0 is warmup
    }
    write("HTTPRoute", [doc], -1);
    await stack.settle(200, cfg.timeoutMs);
  }
  const st = stats(lat);
  csvAppend(
    `${cfg.out}/corner-megavhost-${mode}.csv`,
    ["routes", "reps", "xds_median_ms", "xds_p95_ms", "preload_ms", "runtime_errors"],
    [cfg.k, st.n, st.median.toFixed(3), st.p95.toFixed(3), preloadMs.toFixed(0), stack.runtimeErrors],
  );
  console.log(
    `CORNER mega-vhost ${mode} k=${cfg.k}: route-add xds median=${st.median.toFixed(2)}ms p95=${st.p95.toFixed(2)}ms`,
  );
}

// --- endpoint-heavy: M gateways x 1 route, E endpoints per backend. ---
async function endpointHeavy() {
  write("GatewayClass", [fixtures.gatewayClass()]);
  const gws = [];
  const routes = [];
  const svcs = [];
  const slices = [];
  for (let i = 1; i <= cfg.m; i++) {
    gws.push(gatewayDoc(gwName(i), 8000 + i));
    routes.push(routeDoc(rtName(i), gwName(i), `h-${i}.corner.example.com`, svcName(i)));
    svcs.push(fixtures.webService(svcName(i)));
    slices.push(sliceDoc(svcName(i), addrs(i, cfg.e)));
  }
  const t0 = performance.now();
  write("Gateway", gws);
  write("Service", svcs);
  write("EndpointSlice", slices);
  write("HTTPRoute", routes);
  await converge(cfg.m);
  const preloadMs = performance.now() - t0;

  const lat = [];
  for (let j = 0; j <= cfg.reps; j++) {
    const marker = `10.201.${nonce % 250}.${1 + (j % 250)}`;
    const a = addrs(1, cfg.e);
    a[0] = marker;
    const t = performance.now();
    write("EndpointSlice", [sliceDoc(svcName(1), a)]);
    const at = await taps.eds.waitMatch(
      (d, w) => w > 0 && JSON.stringify(d).includes(marker),
      cfg.timeoutMs,
      `eds ${marker}`,
    );
    if (j > 0) {
      lat.push(at - t);
    }
    await stack.settle(150, cfg.timeoutMs);
  }
  const st = stats(lat);
  csvAppend(
    `${cfg.out}/corner-endpointheavy-${mode}.csv`,
    ["endpoints", "gateways", "reps", "xds_median_ms", "xds_p95_ms", "preload_ms", "runtime_errors"],
    [cfg.e, cfg.m, st.n, st.median.toFixed(3), st.p95.toFixed(3), preloadMs.toFixed(0), stack.runtimeErrors],
  );
  console.log(
    `CORNER endpoint-heavy ${mode} e=${cfg.e}: eps-update xds median=${st.median.toFixed(2)}ms p95=${st.p95.toFixed(2)}ms`,
  );
}

// --- saturation: endpoint updates at a fixed rate; latency distribution. ---
async function saturation() {
  write("GatewayClass", [fixtures.gatewayClass()]);
  const gws = [];
  const routes = [];
  const svcs = [];
  const slices = [];
  for (let i = 1; i <= cfg.m; i++) {
    gws.push(gatewayDoc(gwName(i), 8000 + i));
    routes.push(routeDoc(rtName(i), gwName(i), `h-${i}.corner.example.com`, svcName(i)));
    svcs.push(fixtures.webService(svcName(i)));
    slices.push(sliceDoc(svcName(i), addrs(i, 5)));
  }
  write("Gateway", gws);
  write("Service", svcs);
  write("EndpointSlice", slices);
  write("HTTPRoute", routes);
  await converge(cfg.m);

  // Fire updates round-robin without awaiting; the eds tap resolves each
  // marker's arrival.
  const inflight = new Map(); // marker -> t0
  const lat = [];
  subscribe("bench.xds.eds", (entries) => {
    const now = performance.now();
    for (const [doc, w] of entries) {
      if (w <= 0) continue;
      const s = JSON.stringify(doc);
      for (const [marker, t0] of inflight) {
        if (s.includes(marker)) {
          lat.push(now - t0);
          inflight.delete(marker);
        }
      }
    }
  });

  const total = Math.floor(cfg.rate * cfg.duration);
  let fired = 0;
  const intervalMs = 1000 / cfg.rate;
  const tStart = performance.now();
  await new Promise((done) => {
    const timer = setInterval(() => {
      if (fired >= total) {
        clearInterval(timer);
        done();
        return;
      }
      fired++;
      const i = 1 + (fired % cfg.m);
      const marker = `10.202.${(fired / 250) | 0}.${1 + (fired % 250)}`;
      const a = addrs(i, 5);
      a[0] = marker;
      inflight.set(marker, performance.now());
      write("EndpointSlice", [sliceDoc(svcName(i), a)]);
    }, intervalMs);
  });
  // Drain.
  const drainDeadline = performance.now() + 30000;
  while (inflight.size > 0 && performance.now() < drainDeadline) {
    await dgw.sleep(100);
  }
  const wall = performance.now() - tStart;
  const st = stats(lat);
  const lost = inflight.size;
  csvAppend(
    `${cfg.out}/corner-saturation-${mode}.csv`,
    [
      "rate", "duration_s", "gateways", "fired", "completed", "lost",
      "lat_median_ms", "lat_p95_ms", "lat_max_ms", "wall_ms", "runtime_errors",
    ],
    [
      cfg.rate, cfg.duration, cfg.m, fired, lat.length, lost,
      st.median.toFixed(1), st.p95.toFixed(1), st.max.toFixed(1), wall.toFixed(0), stack.runtimeErrors,
    ],
  );
  console.log(
    `CORNER saturation ${mode} rate=${cfg.rate}/s: completed=${lat.length}/${fired} lost=${lost} ` +
      `lat median=${st.median.toFixed(1)}ms p95=${st.p95.toFixed(1)}ms max=${st.max.toFixed(1)}ms`,
  );
}


// --- amplification: N routes admitted from a label-selected namespace;
// churn an annotation on that namespace (irrelevant to every gateway
// semantic) and measure the interference on a concurrent endpoint-update
// probe, the spurious xDS traffic, and the contender's CPU. ---
async function amplification() {
  const ns = "amp-routes";
  // The churned field: an annotation (outside every dependency set) or a
  // label (inside the admission selector's input, though never matched).
  const churnKind = argv.churn || "annotation";
  const nsDoc = (seq) => ({
    kind: "Namespace",
    metadata: {
      name: ns,
      labels: churnKind === "label" ? { bench: "allowed", seq: `s${seq}` } : { bench: "allowed" },
      annotations: { "bench.l7mp.io/seq": `${seq}` },
    },
  });
  await stack.ensureNamespaces([nsDoc(0)], cfg.timeoutMs);
  write("GatewayClass", [fixtures.gatewayClass()]);
  write("Gateway", [
    fixtures.gateway({
      metadata: { name: gwName(1), namespace: "default" },
      spec: {
        gatewayClassName: "delta-gateway",
        listeners: [{
          name: "web",
          protocol: "HTTP",
          port: 8080,
          allowedRoutes: { namespaces: { from: "Selector", selector: { matchLabels: { bench: "allowed" } } } },
        }],
      },
    }),
  ]);
  const svc = svcName(0);
  write("Service", [{
    kind: "Service",
    metadata: { name: svc, namespace: ns },
    spec: { ports: [{ name: "http", protocol: "TCP", port: 80, targetPort: 80 }] },
  }]);
  const sliceOf = (a) => ({
    kind: "EndpointSlice",
    metadata: { name: `${svc}-eps`, namespace: ns, labels: { "kubernetes.io/service-name": svc } },
    addressType: "IPv4",
    endpoints: [{ addresses: [a], conditions: { ready: true } }],
    ports: [{ name: "http", protocol: "TCP", port: 80 }],
  });
  write("EndpointSlice", [sliceOf("10.20.0.1")]);
  const routes = [];
  for (let k = 1; k <= cfg.k; k++) {
    routes.push(fixtures.httpRoute({
      metadata: { name: rtName(k), namespace: ns },
      spec: {
        parentRefs: [{ name: gwName(1), namespace: "default", sectionName: "web" }],
        hostnames: [`amp-${k}.corner.example.com`],
        rules: [{ matches: [{ path: { type: "PathPrefix", value: "/" } }], backendRefs: [{ name: svc, port: 80 }] }],
      },
    }));
  }
  write("HTTPRoute", routes);
  await converge(cfg.k);

  // The endpoint-update probe: one rep = write a marker address, time its
  // eds delivery.
  let probeSeq = 0;
  async function probe() {
    probeSeq++;
    const marker = `10.21.${(probeSeq / 250) | 0}.${1 + (probeSeq % 250)}`;
    const t = performance.now();
    write("EndpointSlice", [sliceOf(marker)]);
    const at = await taps.eds.waitMatch(
      (d, w) => w > 0 && JSON.stringify(d).includes(marker),
      cfg.timeoutMs,
      `eds ${marker}`,
    );
    return at - t;
  }

  const cpuTicks = () => {
    if (!argv["sut-pid"]) {
      return NaN;
    }
    try {
      const stat = fs.readFileSync(`/proc/${argv["sut-pid"]}/stat`, "utf8").split(" ");
      return Number(stat[13]) + Number(stat[14]);
    } catch (e) {
      return NaN;
    }
  };

  // Baseline: the probe with no churn.
  const baseLat = [];
  for (let j = 0; j <= cfg.reps; j++) {
    const l = await probe();
    if (j > 0) {
      baseLat.push(l);
    }
    await stack.settle(150, cfg.timeoutMs);
  }
  const baseCPU0 = cpuTicks();
  const tBase0 = performance.now();
  await dgw.sleep(2000); // idle CPU reference window
  const baseCPU = (cpuTicks() - baseCPU0) / ((performance.now() - tBase0) / 1000);

  // Churn: annotation updates at RATE for DURATION, probing throughout.
  const xds0 = taps.lds.events + taps.rds.events + taps.cds.events + taps.eds.events;
  const cpu0 = cpuTicks();
  const churnLat = [];
  let fired = 0;
  const total = Math.floor(cfg.rate * cfg.duration);
  const t0 = performance.now();
  const churner = setInterval(() => {
    if (fired >= total) {
      return;
    }
    fired++;
    write("Namespace", [nsDoc(fired)]);
  }, 1000 / cfg.rate);
  while (fired < total) {
    churnLat.push(await probe());
    await dgw.sleep(300);
  }
  clearInterval(churner);
  await stack.settle(1000, cfg.timeoutMs * 5);
  const wallS = (performance.now() - t0) / 1000;
  const cpuRate = (cpuTicks() - cpu0) / wallS;
  const xdsEvents = taps.lds.events + taps.rds.events + taps.cds.events + taps.eds.events - xds0;

  const bs = stats(baseLat);
  const cs = stats(churnLat);
  csvAppend(
    `${cfg.out}/corner-amplification-${churnKind}-${mode}.csv`,
    [
      "routes", "rate", "duration_s", "churn_events",
      "base_median_ms", "base_p95_ms", "churn_median_ms", "churn_p95_ms",
      "idle_cpu_ticks_s", "churn_cpu_ticks_s", "xds_events", "runtime_errors",
    ],
    [
      cfg.k, cfg.rate, cfg.duration, fired,
      bs.median.toFixed(3), bs.p95.toFixed(3), cs.median.toFixed(3), cs.p95.toFixed(3),
      Number.isNaN(baseCPU) ? "" : baseCPU.toFixed(1),
      Number.isNaN(cpuRate) ? "" : cpuRate.toFixed(1),
      xdsEvents, stack.runtimeErrors,
    ],
  );
  console.log(
    `CORNER amplification(${churnKind}) ${mode} n=${cfg.k} rate=${cfg.rate}/s: probe ` +
      `${bs.median.toFixed(2)} -> ${cs.median.toFixed(2)}ms (p95 ${cs.p95.toFixed(2)}ms) ` +
      `cpu ${Number.isNaN(baseCPU) ? "?" : baseCPU.toFixed(1)} -> ${Number.isNaN(cpuRate) ? "?" : cpuRate.toFixed(1)} ticks/s ` +
      `xds_events=${xdsEvents}`,
  );
}

const CASES = { "mega-vhost": megaVhost, "endpoint-heavy": endpointHeavy, saturation: saturation, amplification: amplification };
if (!CASES[cfg.kase]) {
  throw new Error(`unknown case ${cfg.kase}; use ${Object.keys(CASES).join(", ")}`);
}
await CASES[cfg.kase]();
console.log(`CORNER done: case=${cfg.kase} mode=${mode} runtime_errors=${stack.runtimeErrors}`);
console.log(`STATS ${JSON.stringify(runtime.stats())}`);
exit();
