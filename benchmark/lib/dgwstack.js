// The delta-gateway benchmark stack: the production operator (the full
// serialized runtime: watch sources, three circuits, status Patchers and
// xDS targets) hosted entirely in-process, with the Gateway API objects
// as view resources behind the embedded API server and the connector's
// delta-ADS client as the instrumented data plane. Shared by the E5
// macro benchmark's embedded phases (ablation.js, probe.js).

const { OPERATOR, TOPICS } = require("../../apps/dgateway/lib/config.js");
const { buildOperatorSpec } = require("../../apps/dgateway/lib/pipeline.js");
const fixtures = require("../../apps/dgateway/lib/fixtures.js");
const { stableStringify } = require("./measure.js");

const VIEW_GROUP = "gwapi.view.dcontroller.io";
const V = (kind) => `${VIEW_GROUP}/v1alpha1/${kind}`;

const LOAD_KINDS = {
  GatewayClass: "bench.load.gatewayclass",
  Gateway: "bench.load.gateway",
  HTTPRoute: "bench.load.httproute",
  Service: "bench.load.service",
  EndpointSlice: "bench.load.endpointslice",
  Secret: "bench.load.secret",
  Namespace: "bench.load.namespace",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Tap counts and pattern-matches the deliveries on one topic.
class Tap {
  constructor(topic, stack) {
    this.topic = topic;
    this.stack = stack;
    this.events = 0;
    this.bytes = 0;
    this.waiters = [];
    // Operator streams are topics of the operator's private runtime;
    // the bench.* topics are the driver's own, on the ambient runtime.
    if (topic.startsWith("bench.")) {
      subscribe(topic, (entries) => this.onEvent(entries));
    } else {
      stack.handle.subscribe(topic, (entries) => this.onEvent(entries));
    }
  }

  onEvent(entries) {
    if (entries.length > 0) {
      this.stack.lastActivity = performance.now();
    }
    for (const [doc, _w] of entries) {
      this.events++;
      this.bytes += JSON.stringify(doc).length;
    }
    const pending = this.waiters;
    this.waiters = [];
    const now = performance.now();
    for (const w of pending) {
      if (entries.some(([doc, wt]) => w.pred(doc, wt))) {
        w.resolve(now);
      } else {
        this.waiters.push(w);
      }
    }
  }

  // waitMatch resolves with the arrival timestamp of the first entry for
  // which pred(doc, weight) is true.
  waitMatch(pred, timeoutMs, label) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(
        () => reject(new Error(`timeout waiting for ${label} on ${this.topic} after ${timeoutMs}ms`)),
        timeoutMs,
      );
      this.waiters.push({
        pred: pred,
        resolve: (at) => {
          clearTimeout(t);
          resolve(at);
        },
      });
    });
  }
}

// Stack wires the whole controller and returns handles for load writing,
// measurement taps and synchronization.
class Stack {
  // opts.mode: "reconciler" (closed loop, default) | "open" | "sotw" | "smith".
  constructor(opts) {
    this.mode = opts.mode || (opts.reconcile === false ? "open" : "reconciler");
    this.reconcile = this.mode === "reconciler";
    this.sotw = this.mode === "sotw";
    this.smith = this.mode === "smith";
    this.apiPort = opts.apiPort || 18443;
    this.xdsAddr = `127.0.0.1:${opts.xdsPort || 18100}`;
    this.lastActivity = performance.now();
    this.runtimeErrors = 0;

    kubernetes.runtime.start({
      apiServer: { http: true, insecure: true, addr: "127.0.0.1", port: this.apiPort },
    });
    kubernetes.runtime.registerViews([
      V("GatewayClass"), V("Gateway"), V("HTTPRoute"),
      V("BackendTLSPolicy"), V("Service"), V("EndpointSlice"),
      V("Secret"), V("Namespace"),
    ]);

    const onError = (err) => {
      this.runtimeErrors++;
      console.log(`RUNTIME ERROR: ${JSON.stringify(err)}`);
    };
    runtime.onError(onError);

    // The operator's xDS egress server, started under the operator's
    // name before loading so the loader binds the xDS targets to it.
    xds.server.start({ name: OPERATOR, address: this.xdsAddr });

    // The production operator spec, loaded whole, with one difference:
    // there is no real cluster here, the Gateway API objects are view
    // resources behind the embedded API server, so every Kubernetes
    // binding is re-pointed at the view group.
    const spec = buildOperatorSpec({
      bindings: "kubernetes",
      reconcile: this.reconcile,
      sotw: this.sotw,
      smith: this.smith,
      // The Smith predictor's known dead time, in circuit steps.
      smithK: opts.smithK !== undefined ? Number(opts.smithK) : 2,
    });
    for (const src of spec.sources) {
      // The misc tick source is not a cluster binding: it keeps its own
      // group and routes to the misc connector.
      if (src.apiGroup === "misc.connector.dcontroller.io") {
        continue;
      }
      src.apiGroup = VIEW_GROUP;
      src.version = "v1alpha1";
    }
    for (const tgt of spec.targets) {
      if (tgt.type === "Patcher") {
        tgt.apiGroup = VIEW_GROUP;
        tgt.version = "v1alpha1";
      }
    }
    this.handle = runtime.create(OPERATOR, spec);
    this.handle.onError(onError);
    this.handle.start();

    // The load writers: ambient updaters that write the view objects the
    // operator watches.
    for (const [kind, topic] of Object.entries(LOAD_KINDS)) {
      kubernetes.update(topic, { gvk: V(kind) });
    }

    // The instrumented data plane: a delta-ADS client per resource type.
    xds.watch("bench.xds.lds", { type: "lds", address: this.xdsAddr, node: "bench-envoy" });
    xds.watch("bench.xds.rds", { type: "rds", address: this.xdsAddr, node: "bench-envoy" });
    xds.watch("bench.xds.cds", { type: "cds", address: this.xdsAddr, node: "bench-envoy" });
    xds.watch("bench.xds.eds", { type: "eds", address: this.xdsAddr, node: "bench-envoy" });

    this.taps = {
      lds: new Tap("bench.xds.lds", this),
      rds: new Tap("bench.xds.rds", this),
      cds: new Tap("bench.xds.cds", this),
      eds: new Tap("bench.xds.eds", this),
      gatewayClass: new Tap(TOPICS.inputs.gatewayClass, this),
      gateway: new Tap(TOPICS.inputs.gateway, this),
      route: new Tap(TOPICS.inputs.route, this),
    };

    // The latest version of every watched statused object, and the count
    // of status-carrying deliveries (each corresponds to a landed status
    // write; the view cache suppresses no-op updates).
    this.latest = new Map();
    this.statusEvents = 0;
    for (const tap of [this.taps.gatewayClass, this.taps.gateway, this.taps.route]) {
      this.handle.subscribe(tap.topic, (entries) => {
        for (const [doc, w] of entries) {
          if (w > 0) {
            this.latest.set(objKey(doc), doc);
            if (doc.status) {
              this.statusEvents++;
            }
          }
        }
      });
    }
  }

  // subscribe taps one of the operator's streams (a topic of its private
  // runtime).
  subscribe(topic, cb) {
    this.handle.subscribe(topic, cb);
  }

  // write publishes docs to the load updater for the kind.
  write(kind, docs, w) {
    const topic = LOAD_KINDS[kind];
    if (!topic) {
      throw new Error(`no load writer for kind ${kind}`);
    }
    publish(topic, docs.map((d) => [d, w === undefined ? 1 : w]));
  }

  async waitUntil(cond, timeoutMs, label) {
    const t0 = performance.now();
    while (!cond()) {
      if (performance.now() - t0 > timeoutMs) {
        throw new Error(`timeout waiting for ${label} after ${timeoutMs}ms`);
      }
      await sleep(50);
    }
  }

  // settle resolves once no tap has seen an event for quietMs.
  async settle(quietMs, timeoutMs) {
    const t0 = performance.now();
    for (;;) {
      const idle = performance.now() - this.lastActivity;
      if (idle >= quietMs) {
        return;
      }
      if (performance.now() - t0 > timeoutMs) {
        throw new Error(`settle timeout after ${timeoutMs}ms`);
      }
      await sleep(Math.max(20, quietMs - idle));
    }
  }
}

// objKey identifies a watched object.
function objKey(doc) {
  return `${doc.kind}/${doc.metadata.namespace || ""}/${doc.metadata.name}`;
}

// --- Base-configuration fixtures. ---

function benchGateway(name, port) {
  return fixtures.gateway({
    metadata: { name: name, namespace: "default" },
    spec: {
      gatewayClassName: "delta-gateway",
      listeners: [{ name: "web", protocol: "HTTP", port: port }],
    },
  });
}

function benchRoute(name, gw, host, svc) {
  return fixtures.httpRoute({
    metadata: { name: name, namespace: "default" },
    spec: {
      parentRefs: [{ name: gw, sectionName: "web" }],
      hostnames: [host],
      rules: [{ backendRefs: [{ name: svc, port: 80 }] }],
    },
  });
}

function benchSlice(svc, addresses) {
  return fixtures.webEndpointSlice(svc, addresses);
}

// baseConfig builds the M×R world: M gateways, R routes each, one backend
// Service+EndpointSlice per route, plus spare pre-created backends.
function baseConfig(m, r, spareBackends) {
  const gateways = [];
  const routes = [];
  const services = [];
  const slices = [];
  for (let i = 1; i <= m; i++) {
    gateways.push(benchGateway(`gw-${i}`, 8000 + i));
    for (let j = 1; j <= r; j++) {
      const svc = `svc-${i}-${j}`;
      routes.push(benchRoute(`rt-${i}-${j}`, `gw-${i}`, `h-${i}-${j}.example.com`, svc));
      services.push(fixtures.webService(svc));
      slices.push(benchSlice(svc, [`10.1.${i % 250}.${j % 250}`]));
    }
  }
  for (let j = 1; j <= (spareBackends || 0); j++) {
    const svc = `bench-svc-${j}`;
    services.push(fixtures.webService(svc));
    slices.push(benchSlice(svc, [`10.9.0.${j % 250}`]));
  }
  return { gateways, routes, services, slices };
}

// normalizeStatus strips the write-side condition bookkeeping
// (lastTransitionTime is connector-owned process metadata) and serializes
// canonically, for content comparison against an oracle.
function normalizeStatus(status) {
  function strip(v) {
    if (v === null || typeof v !== "object") {
      return v;
    }
    if (Array.isArray(v)) {
      return v.map(strip);
    }
    const out = {};
    for (const [k, val] of Object.entries(v)) {
      if (k === "lastTransitionTime") {
        continue;
      }
      out[k] = strip(val);
    }
    return out;
  }
  return stableStringify(strip(status || null));
}

module.exports = {
  Stack,
  Tap,
  V,
  LOAD_KINDS,
  TOPICS,
  fixtures,
  objKey,
  sleep,
  benchGateway,
  benchRoute,
  benchSlice,
  baseConfig,
  normalizeStatus,
};
