// E5 disturbance rejection: out-of-band status tampering against the live
// contenders, on the shared envtest plant. After the trace world
// converges, the converged statuses become the oracle. Every round one
// EndpointSlice churns (the background workload), then a seeded noise
// source overwrites random Gateway/route statuses through the status
// subresource  -  exactly what an out-of-band writer (a human, a
// misconfigured controller) can do to a real cluster. Drift = the number
// of objects whose status diverges from the oracle (lastTransitionTime
// excluded: connector-owned write-side metadata).
//
// In reconciler mode the watch reports the tampering and the reconciler
// emits the correction; in open-loop mode nothing ever re-reads the
// statuses, so every disturbance is permanent. envoy-gateway reconciles on
// spec changes: whether tampered statuses heal (and when) is exactly what
// this benchmark measures.
//
// Usage:
//   dbsp disturb.js --system=dgw|eg [--mode=reconciler|open|sotw|smith]
//                   [--trace=multiregion-l4] [--regions=10] [--rounds=100]
//                   [--noise=0.3] [--burst=0] [--seed=42]
//                   [--handshake=...] [--eg-handshake=...]
//                   [--xds-address=...] [--out=results] [--timeout-ms=30000]

const minimist = require("minimist");
const fs = require("fs");
const { csvAppend, mulberry32 } = require("../lib/measure.js");
const { K8sStack, TAMPER_KINDS } = require("../lib/k8sstack.js");
const dgw = require("../lib/dgwstack.js");
const traces = require("../lib/traces.js");

const argv = minimist(process.argv.slice(2), {
  string: ["trace", "system", "mode", "out", "handshake", "eg-handshake", "xds-address"],
  default: {
    trace: "multiregion-l4",
    regions: 10,
    system: "dgw",
    mode: "reconciler",
    rounds: 100,
    noise: 0.3,
    burst: 0,
    seed: 42,
    handshake: "/tmp/gw-plant/handshake.json",
    "eg-handshake": "/tmp/eg-serve/handshake.json",
    "xds-address": "",
    out: "results",
    "timeout-ms": 30000,
  },
});
const cfg = {
  trace: argv.trace === "stunner-udp" ? "multiregion-l4" : argv.trace,
  regions: Number(argv.regions),
  system: argv.system,
  rounds: Number(argv.rounds),
  noise: Number(argv.noise),
  burst: Number(argv.burst),
  seed: Number(argv.seed),
  out: argv.out,
  timeoutMs: Number(argv["timeout-ms"]),
};
const mode = cfg.system === "dgw" ? argv.mode : cfg.system;
const rng = mulberry32(cfg.seed);

const world = traces.generate(cfg.trace, cfg.regions, cfg.seed);
const routeKind = world.routes[0].kind;
const homeNs = world.namespaces[0].metadata.name;

const homeSlices = world.slices.filter((s) => s.metadata.namespace === homeNs);

// istio serves Endpoint resources strictly by name: subscribe to the home
// region's clusters (the churn signal).
const xdsResources = {
  eds: homeSlices.map((s) => {
    const svc = s.metadata.labels["kubernetes.io/service-name"];
    const port = s.ports[0].port;
    return `outbound|${port}||${svc}.${homeNs}.svc.cluster.local`;
  }),
};

const stack = new K8sStack({
  system: cfg.system,
  handshake: argv.handshake,
  egHandshake: argv["eg-handshake"],
  xdsAddress: argv["xds-address"],
  watchGateways: [[homeNs, "gw-1"]],
  xdsResources: xdsResources,
  tamper: true,
});

// --- Preload and oracle capture. ---
await stack.ensureNamespaces(world.namespaces, cfg.timeoutMs * 10);
stack.write("GatewayClass", world.gatewayClasses);
stack.write("Gateway", world.gateways);
stack.write("Service", world.services);
stack.write("EndpointSlice", world.slices);
stack.write(routeKind, world.routes);
if (cfg.system !== "dgw") {
  await stack.waitUntil(
    () => stack.acceptedRoutes("") >= world.routes.length,
    cfg.timeoutMs * 10,
    "preload status convergence",
  );
} else {
  await stack.waitUntil(
    () => stack.taps.lds.events >= world.gateways.length && stack.taps.eds.events >= world.services.length,
    cfg.timeoutMs * 10,
    "preload xDS convergence",
  );
}
await stack.settle(1500, cfg.timeoutMs * 10);

// The oracle: every tamperable statused object's converged status.
// (The stack also watches kinds with statuses nobody disturbs or heals
// here  -  Namespace most notably  -  which belong in neither the drift
// count nor the tamper target list.)
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

// drift counts the oracle objects whose current status diverges.
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

// --- Disturbances: out-of-band status writes through the subresource.
// (A real API server keeps status behind the subresource, so a wholesale
// wipe is not expressible from outside; the overwrite below is what an
// out-of-band writer can actually do.) ---
const targets = [...oracle.keys()];

function tamper() {
  const key = targets[Math.floor(rng() * targets.length)];
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

// noiseCount draws the number of disturbances for one round.
function noiseCount() {
  let k = Math.floor(cfg.noise);
  if (rng() < cfg.noise - k) {
    k++;
  }
  return k;
}

// --- Rounds. ---
let churnSeq = 0;
async function churn() {
  const base = homeSlices[churnSeq % homeSlices.length];
  const marker = `10.2.${(churnSeq / 250) % 250 | 0}.${1 + (churnSeq % 250)}`;
  churnSeq++;
  const slice = JSON.parse(JSON.stringify(base));
  slice.endpoints[0].addresses = [marker];
  stack.write("EndpointSlice", [slice]);
  await stack.taps.eds.waitMatch(
    (d, w) => w > 0 && JSON.stringify(d).includes(marker),
    cfg.timeoutMs,
    `eds churn ${marker}`,
  );
}

if (!fs.existsSync(cfg.out)) {
  fs.mkdirSync(cfg.out);
}
const variant = cfg.burst > 0 ? "burst" : `l${cfg.noise}`;
const tracePath = `${cfg.out}/E5-disturb-trace-${mode}-${variant}.csv`;
if (fs.existsSync(tracePath)) {
  fs.rmSync(tracePath);
}

const burstRound = Math.floor(cfg.rounds / 2);
let cumNoise = 0;
const drifts = [];
let healedRound = -1;

for (let t = 1; t <= cfg.rounds; t++) {
  await churn();

  if (cfg.burst > 0) {
    if (t === burstRound) {
      for (let k = 0; k < cfg.burst; k++) {
        tamper();
        cumNoise++;
      }
    }
  } else {
    const k = noiseCount();
    for (let j = 0; j < k; j++) {
      tamper();
      cumNoise++;
    }
  }

  // Give the loop its chance to heal (closed loop converges within
  // milliseconds; open loop has nothing to converge).
  await stack.settle(250, cfg.timeoutMs);

  const d = drift();
  drifts.push(d);
  if (cfg.burst > 0 && t >= burstRound && healedRound < 0 && d === 0) {
    healedRound = t;
  }
  csvAppend(tracePath, ["round", "drift", "cum_noise"], [t, d, cumNoise]);
}

// --- Summary. ---
const half = drifts.slice(Math.floor(drifts.length / 2));
const meanDrift = half.reduce((a, x) => a + x, 0) / (half.length || 1);
const maxDrift = drifts.reduce((a, x) => Math.max(a, x), 0);

if (cfg.burst > 0) {
  const mttr = healedRound < 0 ? -1 : healedRound - burstRound;
  csvAppend(
    `${cfg.out}/E5-disturb-burst.csv`,
    ["mode", "regions", "burst", "burst_round", "healed_round", "mttr_rounds", "runtime_errors"],
    [mode, cfg.regions, cfg.burst, burstRound, healedRound, mttr, stack.runtimeErrors],
  );
  console.log(
    `E5 disturb mode=${mode} burst=${cfg.burst}: mttr=${mttr < 0 ? "never" : mttr + " round(s)"} ` +
      `max_drift=${maxDrift} runtime_errors=${stack.runtimeErrors}`,
  );
} else {
  csvAppend(
    `${cfg.out}/E5-disturb-steady.csv`,
    ["mode", "lambda", "regions", "rounds", "mean_drift", "max_drift", "cum_noise", "runtime_errors"],
    [mode, cfg.noise, cfg.regions, cfg.rounds, meanDrift.toFixed(2), maxDrift, cumNoise, stack.runtimeErrors],
  );
  console.log(
    `E5 disturb mode=${mode} noise=${cfg.noise}: mean_drift(2nd half)=${meanDrift.toFixed(2)} ` +
      `max=${maxDrift} injected=${cumNoise} runtime_errors=${stack.runtimeErrors}`,
  );
}
console.log(`STATS ${JSON.stringify(runtime.stats())}`);
exit();
