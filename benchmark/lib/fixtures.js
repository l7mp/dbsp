// Synthetic Service/Pod fixtures for the endpoints-controller benchmarks.
//
// Three base-config shapes exercise three complexity regimes of the same
// controller:
//
//   join:   1 Service selecting all N Pods, no aggregation. The output is
//           one row per (service, pod) pair, so a single-pod delta touches
//           a single O(1)-sized output row.
//   pair:   N Services, each selecting exactly one of N Pods (1:1), with
//           @groupBy per service. Groups have size 1; the join dominates.
//   fanout: 1 Service selecting all N Pods, with @groupBy. One group of
//           size N: the output document itself is O(N), which lower-bounds
//           every mode.

// podIP maps an index to a deterministic RFC1918 address.
function podIP(i) {
  const q = Math.floor(i / 250);
  const r = (i % 250) + 1;
  return `10.${Math.floor(q / 250)}.${q % 250}.${r}`;
}

function pod(name, app, ip) {
  return {
    kind: "Pod",
    metadata: { name: name, labels: { app: app } },
    status: { podIP: ip },
  };
}

function svc(name, app) {
  return {
    kind: "Service",
    metadata: { name: name },
    spec: { selector: { app: app } },
  };
}

// base returns { pods, services, matches } for the case at size n. matches
// is a predicate identifying one output document guaranteed to be present
// once the base config has fully landed; the driver waits on it as the
// base-load barrier (a content signal, since an empty circuit step carries
// no output and is suppressed before it reaches the output topic).
// spare extra pods are preloaded in the pair case so that measured deltas
// (svc additions) find their pod already in the state.
function base(kase, n, spare) {
  const pods = [];
  const services = [];
  let matches;
  switch (kase) {
    case "join":
      for (let i = 1; i <= n; i++) {
        pods.push(pod(`pod-${i}`, "web", podIP(i)));
      }
      services.push(svc("web", "web"));
      // join emits one row per pod; pod-1 is always among them.
      matches = (d) => d.pod === "pod-1";
      break;
    case "fanout":
      for (let i = 1; i <= n; i++) {
        pods.push(pod(`pod-${i}`, "web", podIP(i)));
      }
      services.push(svc("web", "web"));
      // fanout emits one group of size n; the base has landed once the
      // group holds all n endpoints.
      matches = (d) => Array.isArray(d.endpoints) && d.endpoints.length >= n;
      break;
    case "pair":
      for (let i = 1; i <= n + (spare || 0); i++) {
        pods.push(pod(`pod-${i}`, `app-${i}`, podIP(i)));
      }
      for (let i = 1; i <= n; i++) {
        services.push(svc(`svc-${i}`, `app-${i}`));
      }
      // pair emits one group per service; svc-1 is always among them.
      matches = (d) => d.metadata && d.metadata.name === "svc-1";
      break;
    default:
      throw new Error(`unknown case: ${kase}`);
  }
  return { pods, services, matches };
}

// delta returns the j-th measured delta for the case as
// { topic, doc, matches(outDoc) } where matches identifies the output
// entry whose arrival marks the delta as landed.
function delta(kase, n, j) {
  switch (kase) {
    case "join":
    case "fanout": {
      const name = `pod-${n + j}`;
      return {
        topic: "pods",
        doc: pod(name, "web", podIP(n + j)),
        matches:
          kase === "join"
            ? (d) => d.pod === name
            : (d) => Array.isArray(d.endpoints) && d.endpoints.length > n,
      };
    }
    case "pair": {
      const name = `svc-${n + j}`;
      return {
        topic: "services",
        doc: svc(name, `app-${n + j}`),
        matches: (d) => d.metadata && d.metadata.name === name,
      };
    }
    default:
      throw new Error(`unknown case: ${kase}`);
  }
}

module.exports = { podIP, pod, svc, base, delta };
