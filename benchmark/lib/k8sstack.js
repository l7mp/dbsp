// The k8s-native benchmark stack: the single driver/measurement surface
// for the macro benchmarks. Every system under test runs OUT OF PROCESS
// against the shared envtest plant (kube-apiserver + etcd, provisioned by
// the plant launcher); the driver writes Gateway API objects through
// the plant kubeconfig with sharded native-GVK updaters, observes statuses
// through kubernetes.watch, and measures the data plane with the
// connector's own delta-ADS clients:
//
//   - system "dgw": plaintext wildcard client against delta-gateway's xDS
//     server (one client observes every resource);
//   - system "eg": mTLS clients against envoy-gateway's xDS server, one
//     per watched gateway (envoy-gateway scopes snapshots by matching
//     node.cluster against its IR key "{ns}/{gateway}"); all clients
//     publish into the same per-type topics, keeping the taps identical.
//
// The write path, the status taps, the clocks and the convergence
// predicates are byte-for-byte the same for every contender; only the xDS
// endpoint (and its transport) differs.

const fs = require("fs");
const { Tap, objKey, sleep } = require("./dgwstack.js");
const { ShardedLoader } = require("./loader.js");

const NATIVE_GVKS = {
  GatewayClass: "gateway.networking.k8s.io/v1/GatewayClass",
  Gateway: "gateway.networking.k8s.io/v1/Gateway",
  HTTPRoute: "gateway.networking.k8s.io/v1/HTTPRoute",
  Service: "v1/Service",
  EndpointSlice: "discovery.k8s.io/v1/EndpointSlice",
  Secret: "v1/Secret",
  Namespace: "v1/Namespace",
  Pod: "v1/Pod",
};

// The kinds whose statuses the disturbance benchmark tampers with.
const TAMPER_KINDS = ["Gateway", "HTTPRoute"];

class K8sStack {
  // opts.system: "dgw" (default), "eg" or "istio".
  // opts.handshake: path to the plant handshake JSON (kubeconfig).
  // opts.xdsAddress: the contender's xDS server address (systems dgw and
  //   istio; empty selects the system's default port; envoy-gateway
  //   publishes its own in the serve handshake).
  // opts.egHandshake: path to the envoy-gateway serve handshake JSON with
  //   the xDS address, client certs and the gateway class (system eg).
  // opts.watchGateways: [ns, name] pairs whose xDS the measurement
  //   clients subscribe to (systems eg and istio; the dgw client 0 sees
  //   everything).
  // opts.xdsResources: {eds: [...], rds: [...]} explicit resource-name
  //   subscriptions per type (system istio: Endpoint and Route resources
  //   are served by name only, per the xDS spec).
  // opts.tamper: create status patchers for out-of-band tampering.
  constructor(opts) {
    this.system = opts.system || "dgw";
    this.mode = this.system;
    this.lastActivity = performance.now();
    this.runtimeErrors = 0;

    const hs = JSON.parse(fs.readFileSync(opts.handshake || "/tmp/gw-plant/handshake.json", "utf8"));
    this.handshake = hs;
    this.eg = null;
    if (this.system === "eg") {
      this.eg = JSON.parse(fs.readFileSync(opts.egHandshake || "/tmp/eg-serve/handshake.json", "utf8"));
    }

    // The driver is a load generator: give it a driver-sized client
    // budget (the systems under test keep their own shipped defaults).
    kubernetes.runtime.start({
      kubeconfig: hs.kubeconfig,
      qps: opts.qps || 500,
      burst: opts.burst || 1000,
      userAgent: opts.userAgent || "bench-driver",
    });
    runtime.onError((err) => {
      this.runtimeErrors++;
      console.log(`RUNTIME ERROR: ${JSON.stringify(err)}`);
    });

    // Load writers (native GVKs, sharded) and watch taps.
    this.loaders = {};
    for (const [kind, gvk] of Object.entries(NATIVE_GVKS)) {
      this.loaders[kind] = new ShardedLoader({
        gvk: gvk,
        prefix: `bench.load.${kind.toLowerCase()}`,
        shards: 4,
      });
    }
    kubernetes.watch("bench.watch.namespace", { gvk: NATIVE_GVKS.Namespace });
    kubernetes.watch("bench.watch.gatewayclass", { gvk: NATIVE_GVKS.GatewayClass });
    kubernetes.watch("bench.watch.gateway", { gvk: NATIVE_GVKS.Gateway });
    kubernetes.watch("bench.watch.route", { gvk: NATIVE_GVKS.HTTPRoute });

    // Out-of-band status tampering (the disturbance benchmark): documents
    // carrying only metadata + status go through the status subresource,
    // exactly what an out-of-band writer with API access can do.
    this.tamperTopics = {};
    if (opts.tamper) {
      for (const kind of TAMPER_KINDS) {
        const topic = `bench.tamper.${kind.toLowerCase()}`;
        kubernetes.patch(topic, { gvk: NATIVE_GVKS[kind] });
        this.tamperTopics[kind] = topic;
      }
    }

    // Measurement clients.
    let seq = 0;
    if (this.system === "eg") {
      const tls = {
        cert: this.eg.clientCert,
        key: this.eg.clientKey,
        ca: this.eg.ca,
        serverName: this.eg.serverName,
      };
      this.addXdsClient = (ns, gateway) => {
        seq++;
        for (const type of ["lds", "rds", "cds", "eds"]) {
          xds.watch(`bench.xds.${type}`, {
            type: type,
            address: this.eg.xdsAddress,
            node: `bench-envoy-${seq}-${type}`,
            nodeCluster: `${ns}/${gateway}`,
            tls: tls,
          });
        }
      };
      for (const [ns, gw] of opts.watchGateways || []) {
        this.addXdsClient(ns, gw);
      }
    } else if (this.system === "istio") {
      // istiod scopes gateway config by the proxy's workload identity: one
      // client per watched gateway impersonates that gateway's proxy (a
      // router node whose metadata labels carry the gateway-name label).
      // The proxy IP doubles as the gateway workload's endpoint address in
      // the companion Service the write path provisions.
      this.xdsAddr = opts.xdsAddress || "127.0.0.1:15010";
      this.istioGwIP = new Map();
      const resources = opts.xdsResources || {};
      this.addXdsClient = (ns, gateway) => {
        seq++;
        const ip = `10.88.${(seq / 250) | 0}.${1 + (seq % 250)}`;
        this.istioGwIP.set(`${ns}/${gateway}`, ip);
        for (const type of ["lds", "rds", "cds", "eds"]) {
          xds.watch(`bench.xds.${type}`, {
            type: type,
            address: this.xdsAddr,
            node: `router~${ip}~bench-${gateway}.${ns}~${ns}.svc.cluster.local`,
            nodeMetadata: {
              NAMESPACE: ns,
              CLUSTER_ID: "Kubernetes",
              ISTIO_VERSION: "1.27.9",
              LABELS: { "gateway.networking.k8s.io/gateway-name": gateway },
            },
            resources: resources[type],
          });
        }
      };
      for (const [ns, gw] of opts.watchGateways || []) {
        this.addXdsClient(ns, gw);
      }
    } else {
      this.xdsAddr = opts.xdsAddress || "127.0.0.1:19000";
      // Client 0: the wildcard measurement client.
      for (const type of ["lds", "rds", "cds", "eds"]) {
        xds.watch(`bench.xds.${type}`, { type: type, address: this.xdsAddr, node: "bench-envoy" });
      }
      // Extra clients (fan-out / storm) subscribe like more Envoys and
      // publish into their own per-client topics.
      this.addXdsClient = (topicPrefix) => {
        seq++;
        const taps = {};
        for (const type of ["lds", "rds", "cds", "eds"]) {
          const topic = `${topicPrefix || "bench.xds.c" + seq}.${type}`;
          xds.watch(topic, { type: type, address: this.xdsAddr, node: `bench-envoy-${seq}` });
          taps[type] = new Tap(topic, this);
        }
        return taps;
      };
    }

    this.taps = {
      lds: new Tap("bench.xds.lds", this),
      rds: new Tap("bench.xds.rds", this),
      cds: new Tap("bench.xds.cds", this),
      eds: new Tap("bench.xds.eds", this),
      namespace: new Tap("bench.watch.namespace", this),
      gatewayClass: new Tap("bench.watch.gatewayclass", this),
      gateway: new Tap("bench.watch.gateway", this),
      route: new Tap("bench.watch.route", this),
    };

    this.latest = new Map();
    this.statusEvents = 0;
    for (const tap of [this.taps.namespace, this.taps.gatewayClass, this.taps.gateway, this.taps.route]) {
      subscribe(tap.topic, (entries) => {
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

  // write adapts and publishes docs to the native updaters. For
  // envoy-gateway, GatewayClass writes are dropped (the serve mode owns
  // the class) and gateways are re-pointed at envoy-gateway's class. For
  // istio, gateways run in the documented manual-deployment mode: the
  // class is istiod's own auto-created "istio", spec.addresses names a
  // companion Service the driver provisions alongside (selector = the
  // gateway-name label, endpoint = the measurement client's proxy IP), so
  // the impersonated proxy is the gateway's workload.
  write(kind, docs, w) {
    let adapted = docs;
    if (this.system === "eg" || this.system === "istio") {
      if (kind === "GatewayClass") {
        return;
      }
      adapted = docs.map((d) => {
        if (kind === "Gateway") {
          const c = JSON.parse(JSON.stringify(d));
          if (this.system === "eg") {
            c.spec.gatewayClassName = this.eg.gatewayClass;
          } else {
            c.spec.gatewayClassName = "istio";
            c.spec.addresses = [{ type: "Hostname", value: `gw-svc-${c.metadata.name}` }];
            this.writeGatewayCompanions(c, w === undefined ? 1 : w);
          }
          return c;
        }
        return d;
      });
    }
    this.loaders[kind].write(adapted, w === undefined ? 1 : w);
  }

  // writeGatewayCompanions provisions (or retracts) the manual-mode
  // gateway workload for one istio gateway: the Service its
  // spec.addresses hostname names, the workload's endpoint (the
  // measurement client's proxy IP), and a Pod named after the
  // impersonated proxy id - istiod resolves the proxy's workload identity
  // (labels, service targets) through the pod named in the node id, which
  // makes the binding independent of connect/create ordering.
  writeGatewayCompanions(gw, w) {
    const ns = gw.metadata.namespace;
    const name = gw.metadata.name;
    const key = `${ns}/${name}`;
    if (!this.istioGwIP.has(key)) {
      this.istioGwIP.set(key, `10.89.${(this.istioGwIP.size / 250) | 0}.${1 + (this.istioGwIP.size % 250)}`);
    }
    const ip = this.istioGwIP.get(key);
    const ports = (gw.spec.listeners || []).map((l, i) => ({
      name: l.name || `l${i}`,
      protocol: "TCP",
      port: l.port,
      targetPort: l.port,
    }));
    this.loaders.Service.write(
      [{
        kind: "Service",
        metadata: { name: `gw-svc-${name}`, namespace: ns },
        spec: { selector: { "gateway.networking.k8s.io/gateway-name": name }, ports: ports },
      }],
      w,
    );
    this.loaders.EndpointSlice.write(
      [{
        kind: "EndpointSlice",
        metadata: {
          name: `gw-svc-${name}-eps`,
          namespace: ns,
          labels: { "kubernetes.io/service-name": `gw-svc-${name}` },
        },
        addressType: "IPv4",
        endpoints: [{ addresses: [ip], conditions: { ready: true } }],
        ports: ports.map((p) => ({ name: p.name, protocol: "TCP", port: p.port })),
      }],
      w,
    );
    this.loaders.Pod.write(
      [{
        kind: "Pod",
        metadata: {
          name: `bench-${name}`,
          namespace: ns,
          labels: { "gateway.networking.k8s.io/gateway-name": name },
        },
        spec: { containers: [{ name: "proxy", image: "bench:proxy" }] },
      }],
      w,
    );
  }

  // tamperStatus overwrites one object's status through the status
  // subresource, bypassing the controller.
  tamperStatus(kind, doc) {
    const topic = this.tamperTopics[kind];
    if (!topic) {
      throw new Error(`no tamper patcher for ${kind} (construct the stack with tamper: true)`);
    }
    publish(topic, [[doc, 1]]);
  }

  // ensureNamespaces writes namespace documents and waits until every one
  // is observed back through the watch. Connectors do not share a client,
  // so nothing orders a namespace create ahead of its member objects'
  // creates except this explicit causal barrier - write namespaces through
  // it before writing anything that lives in them.
  async ensureNamespaces(docs, timeoutMs) {
    this.write("Namespace", docs);
    await this.waitUntil(
      () => docs.every((d) => this.latest.has(`Namespace//${d.metadata.name}`)),
      timeoutMs,
      "namespace barrier",
    );
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

  // acceptedRoutes counts watched routes carrying an Accepted=True parent
  // condition - the status-side convergence signal (for envoy-gateway the
  // xDS taps only see the subscribed gateways).
  acceptedRoutes(namePrefix) {
    let n = 0;
    for (const [key, doc] of this.latest) {
      if (!key.includes("Route/")) {
        continue;
      }
      if (namePrefix && !doc.metadata.name.startsWith(namePrefix)) {
        continue;
      }
      const parents = doc.status?.parents || [];
      if (
        parents.some((p) =>
          (p.conditions || []).some((c) => c.type === "Accepted" && c.status === "True"),
        )
      ) {
        n++;
      }
    }
    return n;
  }
}

module.exports = { K8sStack, NATIVE_GVKS, TAMPER_KINDS };
