// Diagnostic probe for the reconciler cold-start storm: preloads the base
// configuration like bench.js and records, per status-output topic event,
// the delivered Z-set size and per-content repeat counts. Distinguishes
// echo-lag re-emission (same content delivered many times), never-cancelling
// corrections (repeats that outlive the run) and genuine desired-state churn
// (many distinct contents per object).
//
// Usage: dbsp probe.js [--gateways=10] [--routes=5] [--dgw-mode=reconciler]
//                      [--api-port=18445] [--xds-port=18105]

const minimist = require("minimist");
const dgw = require("../lib/dgwstack.js");
const { TOPICS } = require("../../apps/dgateway/lib/config.js");
const { stableStringify } = require("../lib/measure.js");

const argv = minimist(process.argv.slice(2), {
  string: ["dgw-mode"],
  default: { gateways: 10, routes: 5, "dgw-mode": "reconciler", "api-port": 18445, "xds-port": 18105 },
});
const m = Number(argv.gateways);
const r = Number(argv.routes);

const fs = require("fs");

const stack = new dgw.Stack({
  mode: argv["dgw-mode"],
  apiPort: Number(argv["api-port"]),
  xdsPort: Number(argv["xds-port"]),
});


// Per-topic delivery stats on the k8s status outputs (what the patchers see).
const probes = new Map();
for (const [kind, topic] of Object.entries(TOPICS.status)) {
  const p = {
    kind: kind,
    events: 0,
    entries: 0,
    maxEvent: 0,
    perContent: new Map(), // content -> deliveries
    perKey: new Map(), // object key -> Set of distinct contents
  };
  probes.set(topic, p);
  stack.subscribe(topic, (entries) => {
    p.events++;
    if (kind === "httpRoute") {
      stepIdx++;
    }
    p.entries += entries.length;
    if (entries.length > p.maxEvent) {
      p.maxEvent = entries.length;
    }
    for (const [doc, w] of entries) {
      const content = `${w > 0 ? "+" : "-"}${doc.kind ? "" : "[kindless]"}${stableStringify(doc)}`;
      p.perContent.set(content, (p.perContent.get(content) || 0) + 1);
      const key = `${doc.kind || "?"}/${doc.metadata.namespace || ""}/${doc.metadata.name}`;
      if (!p.perKey.has(key)) {
        p.perKey.set(key, new Set());
      }
      p.perKey.get(key).add(content);
      const t = traceEntry(key);
      if (t.firstSeen < 0) {
        t.firstSeen = stepIdx;
      }
      t.lastSeen = stepIdx;
    }
  });
}

// The observed-side curation, keyed like the desired side. Arrival is
// indexed against the step counter (events seen on the httpRoute status
// topic) so the gap between "echo entered the input queue" and "correction
// left U" measures the circuit backlog in steps.
let stepIdx = 0;
const trace = new Map(); // key -> {firstSeen, lastSeen, echoAt}
function traceEntry(key) {
  if (!trace.has(key)) {
    trace.set(key, { firstSeen: -1, lastSeen: -1, echoAt: -1 });
  }
  return trace.get(key);
}
const observedContents = new Map();
for (const topic of Object.values(TOPICS.observed)) {
  stack.subscribe(topic, (entries) => {
    for (const [doc, w] of entries) {
      const key = `${doc.kind || "?"}/${doc.metadata.namespace || ""}/${doc.metadata.name}`;
      if (!observedContents.has(key)) {
        observedContents.set(key, new Set());
      }
      observedContents.get(key).add(`${w > 0 ? "+" : "-"}${stableStringify(doc)}`);
      const t = traceEntry(key);
      if (w > 0 && doc.status && t.echoAt < 0) {
        t.echoAt = stepIdx;
      }
    }
  });
}

const gateways = [];
const routes = [];
const services = [];
const slices = [];
for (let i = 1; i <= m; i++) {
  gateways.push(dgw.benchGateway(`gw-${i}`, 8000 + i));
  for (let j = 1; j <= r; j++) {
    const svc = `svc-${i}-${j}`;
    routes.push(dgw.benchRoute(`rt-${i}-${j}`, `gw-${i}`, `h-${i}-${j}.example.com`, svc));
    services.push(dgw.fixtures.webService(svc));
    slices.push(dgw.benchSlice(svc, [`10.1.${i % 250}.${j % 250}`]));
  }
}

async function main() {
  const t0 = performance.now();
  stack.write("Namespace", [{ kind: "Namespace", metadata: { name: "default" } }]);
  stack.write("GatewayClass", [dgw.fixtures.gatewayClass()]);
  stack.write("Gateway", gateways);
  stack.write("Service", services);
  stack.write("EndpointSlice", slices);
  stack.write("HTTPRoute", routes);
  await stack.waitUntil(
    () => stack.taps.lds.events >= m && stack.taps.eds.events >= m * r,
    600000,
    "preload xDS convergence",
  );
  await stack.settle(1500, 600000);
  const dt = performance.now() - t0;

  console.log(`PROBE mode=${argv["dgw-mode"]} m=${m} r=${r} preload=${dt.toFixed(0)}ms status_writes=${stack.statusEvents}`);
  for (const p of probes.values()) {
    if (p.events === 0) {
      continue;
    }
    let repeats = 0; // deliveries beyond the first per content
    let repeated = 0; // contents delivered more than once
    let maxRep = 0;
    let maxRepContent = "";
    for (const [content, n] of p.perContent) {
      repeats += n - 1;
      if (n > 1) {
        repeated++;
      }
      if (n > maxRep) {
        maxRep = n;
        maxRepContent = content;
      }
    }
    let churnMax = 0;
    let churnKey = "";
    for (const [key, contents] of p.perKey) {
      if (contents.size > churnMax) {
        churnMax = contents.size;
        churnKey = key;
      }
    }
    console.log(
      `TOPIC ${p.kind}: events=${p.events} entries=${p.entries} maxEvent=${p.maxEvent} ` +
        `uniqueContents=${p.perContent.size} repeatedContents=${repeated} redundantDeliveries=${repeats} ` +
        `maxRepeat=${maxRep} objects=${p.perKey.size} maxDistinctPerObject=${churnMax} (${churnKey})`,
    );
    if (maxRep > 1) {
      console.log(`  top repeat sample: ${maxRepContent.slice(0, 220)}`);
    }
  }

  // Dump the desired contents and the observed-curation contents for one
  // gateway and one route: if the curated observation never equals the
  // desired document canonically, the correction never cancels in U.
  const dumps = [];
  for (const p of probes.values()) {
    for (const [key, contents] of p.perKey) {
      if (key.endsWith("/gw-1") || key.endsWith("/rt-1-1")) {
        dumps.push(`=== DESIRED ${key} (topic ${p.kind}): ${contents.size} distinct contents ===`);
        dumps.push(...contents);
      }
    }
  }
  for (const [key, contents] of observedContents) {
    if (key.endsWith("/gw-1") || key.endsWith("/rt-1-1")) {
      dumps.push(`=== OBSERVED ${key}: ${contents.size} distinct contents ===`);
      dumps.push(...contents);
    }
  }
  fs.writeFileSync("/tmp/probe-churn.txt", dumps.join("\n"));
  console.log("PROBE dump written to /tmp/probe-churn.txt");
  console.log(`STATS ${JSON.stringify(runtime.stats())}`);

  // Correction lifecycle: firstSeen (entered U), echoAt (write echo entered
  // the input queue), lastSeen (left U)  -  all in httpRoute-topic step
  // indices; lastSeen - echoAt is the backlog the echo had to queue behind.
  const samples = ["HTTPRoute/default/rt-1-1", "HTTPRoute/default/rt-5-3", "HTTPRoute/default/rt-10-5", "Gateway/default/gw-1", "Gateway/default/gw-10"];
  for (const key of samples) {
    const t = trace.get(key);
    if (t) {
      console.log(`TRACE ${key}: firstSeen=${t.firstSeen} echoAt=${t.echoAt} lastSeen=${t.lastSeen} residencyAfterEcho=${t.lastSeen - t.echoAt}`);
    }
  }
}

main().then(
  () => exit(0),
  (e) => {
    console.log(`PROBE FAILED: ${e}`);
    exit(1);
  },
);
