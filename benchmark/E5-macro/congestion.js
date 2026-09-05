// E5 congestion: the reconciliation loop behind a throttled client budget.
//
// A real controller's writes queue behind its client-side rate limiter and
// behind a congested apiserver, so the watch confirms each correction
// LATE: a real, tunable feedback dead time on a real apiserver. run.sh
// starts the delta-gateway SUT with a reduced --qps budget; after the
// world converges, this driver tampers a burst of B statuses at once and
// measures how long the loop takes to heal them all through the throttle.
//
// The point of the sweep is the actuation count, not just the latency: the
// closed loop (Reconciler) keeps the whole outstanding correction U alive
// until the watch confirms it, so under congestion it keeps trying to
// re-actuate corrections that are already in flight; the smith loop
// (SmithPredictor) emits every correction exactly once and silently
// retires the echoes, however late they arrive  -  no duplicate ever reaches
// the wire, at any qps. The per-point apiserver request log gives the
// PATCH counts (phase 5 summarizes them next to this driver's heal times;
// the SUT and the driver are separate user agents in the log).
//
// Usage:
//   dbsp congestion.js --system=dgw --mode=reconciler|smith --qps=<label>
//                      [--trace=multiregion-l4] [--regions=10] [--burst=10]
//                      [--seed=42] [--handshake=...] [--xds-address=...]
//                      [--out=results] [--timeout-ms=30000]

const minimist = require("minimist");
const fs = require("fs");
const { csvAppend, mulberry32 } = require("../lib/measure.js");
const { K8sStack, TAMPER_KINDS } = require("../lib/k8sstack.js");
const dgw = require("../lib/dgwstack.js");
const traces = require("../lib/traces.js");

const argv = minimist(process.argv.slice(2), {
  string: ["trace", "system", "mode", "variant", "out", "handshake", "eg-handshake", "xds-address"],
  default: {
    trace: "multiregion-l4",
    regions: 10,
    system: "dgw",
    mode: "reconciler",
    burst: 10,
    qps: 0,
    seed: 42,
    handshake: "/tmp/gw-plant/handshake.json",
    "eg-handshake": "/tmp/eg-serve/handshake.json",
    "xds-address": "",
    out: "results",
    "timeout-ms": 30000,
  },
});
const cfg = {
  trace: argv.trace,
  regions: Number(argv.regions),
  system: argv.system,
  burst: Number(argv.burst),
  qps: Number(argv.qps), // label only: run.sh started the SUT with it
  seed: Number(argv.seed),
  out: argv.out,
  timeoutMs: Number(argv["timeout-ms"]),
};
const mode = (cfg.system === "dgw" ? argv.mode : cfg.system) + (argv.variant ? `-${argv.variant}` : "");
const rng = mulberry32(cfg.seed);

const world = traces.generate(cfg.trace, cfg.regions, cfg.seed);
const routeKind = world.routes[0].kind;
const homeNs = world.namespaces[0].metadata.name;

const stack = new K8sStack({
  system: cfg.system,
  handshake: argv.handshake,
  egHandshake: argv["eg-handshake"],
  xdsAddress: argv["xds-address"],
  watchGateways: [[homeNs, "gw-1"]],
  tamper: true,
});

// --- Preload and oracle capture (the disturbance benchmark's contract). ---
await stack.ensureNamespaces(world.namespaces, cfg.timeoutMs * 10);
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

const oracle = new Map();
for (const [key, doc] of stack.latest) {
  if (doc.status && TAMPER_KINDS.some((k) => key.startsWith(`${k}/`))) {
    oracle.set(key, dgw.normalizeStatus(doc.status));
  }
}
if (oracle.size < world.gateways.length + world.routes.length) {
  throw new Error(`oracle too small: ${oracle.size} statused objects`);
}
console.log(`converged: oracle=${oracle.size} statused objects`);

function drift() {
  let d = 0;
  for (const [key, want] of oracle) {
    const doc = stack.latest.get(key);
    if (!doc || dgw.normalizeStatus(doc.status) !== want) {
      d++;
    }
  }
  return d;
}

// --- The burst: B distinct targets tampered back to back, then the heal
// clock runs until every status matches the oracle again. ---
const targets = [...oracle.keys()];
for (let i = targets.length - 1; i > 0; i--) {
  const j = Math.floor(rng() * (i + 1));
  [targets[i], targets[j]] = [targets[j], targets[i]];
}
const picks = targets.slice(0, Math.min(cfg.burst, targets.length));

const t0 = new Date().toISOString();
const start = performance.now();
for (const key of picks) {
  const cur = stack.latest.get(key);
  const doc = {
    kind: cur.kind,
    metadata: { name: cur.metadata.name, namespace: cur.metadata.namespace },
    status:
      cur.kind === "Gateway"
        ? { conditions: [{ type: "Accepted", status: "False", reason: "Tampered", message: "bench tamper" }] }
        : { parents: [] },
  };
  stack.tamperStatus(cur.kind, doc);
}

// The tamper patches travel through the driver's own (unthrottled)
// patcher and land asynchronously: drift() is still zero right after
// issuing them, and the stack's activity clock still points at the
// preload. First wait for the disturbance to become visible  -  this also
// arms the activity clock  -  then confirm the heal: drift must be zero
// AND stay zero through a quiet window (a mid-burst dip cannot confirm,
// because the ongoing tamper/heal watch traffic keeps the window open).
// The heal clock stops at the stack's last watch activity on
// confirmation  -  the last heal write landing is the last event before
// the quiet window.
await stack.waitUntil(() => drift() > 0, cfg.timeoutMs, "tamper visible");
for (;;) {
  await stack.waitUntil(() => drift() === 0, cfg.timeoutMs * 10, "congestion heal");
  await stack.settle(1000, cfg.timeoutMs * 10);
  if (drift() === 0) {
    break;
  }
}
const healMs = Math.max(0, stack.lastActivity - start);
// Let the loop's own late echoes land before the point is torn down: any
// duplicate actuation still in the throttle queue must reach the request
// log to be counted.
await stack.settle(2000, cfg.timeoutMs * 10);
const t2 = new Date().toISOString();

if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}
csvAppend(
  `${cfg.out}/E5-congestion-${mode}.csv`,
  ["mode", "qps", "regions", "burst", "heal_ms", "t0", "t2", "runtime_errors"],
  [mode, cfg.qps, cfg.regions, picks.length, healMs.toFixed(0), t0, t2, stack.runtimeErrors],
);
console.log(
  `E5 congestion mode=${mode} qps=${cfg.qps} burst=${picks.length}: ` +
    `heal=${healMs.toFixed(0)}ms runtime_errors=${stack.runtimeErrors}`,
);
console.log(`STATS ${JSON.stringify(runtime.stats())}`);
exit(0);
