// Benchmark trace generator: size-parameterized Gateway API worlds whose
// structure is modeled on real deployments. Every shape is deterministic
// (seeded PRNG) so a (spec, regions, seed) triple names a reproducible
// world; only the structure comes from the source deployments - names and
// addresses are synthetic.
//
// Shapes:
//
//   multiregion - modeled on a production multi-region L4 gateway: one
//     shared GatewayClass, then one namespace per region, each with one
//     Gateway carrying a single listener and one shallow route to one
//     backend Service (1:1:1 tuples, shallow-wide). The tuples are carried
//     as HTTP listener + HTTPRoute: the operator's declared surface is
//     HTTP-only, and at the control plane the two shapes cost the same
//     (one listener, one route row, one cluster, one load assignment);
//     the topology and the churn model are the L4 gateway's.
//   sockShop - modeled on the Sock Shop reference application: R regions,
//     each one namespace with one Gateway (single HTTP listener) and one
//     path-prefix HTTPRoute per public service (13 services), each backed
//     by its own Service with 2-3 endpoints. One shared GatewayClass.
//     Scales the ROUTES-PER-GATEWAY dimension (1:13, deep).

const { GATEWAY_API_GROUP } = require("../../apps/dgateway/lib/config.js");
const fixtures = require("../../apps/dgateway/lib/fixtures.js");
const { mulberry32 } = require("./measure.js");

// draw picks a value from a [[value, weight], ...] histogram.
function draw(rng, hist) {
  const total = hist.reduce((a, [, w]) => a + w, 0);
  let x = rng() * total;
  for (const [v, w] of hist) {
    x -= w;
    if (x <= 0) {
      return v;
    }
  }
  return hist[hist.length - 1][0];
}

// ip4 maps indices onto a deterministic RFC 1918 address.
function ip4(a, b, c) {
  return `10.${a % 250}.${b % 250}.${1 + (c % 250)}`;
}

function namespaceDoc(name) {
  return { kind: "Namespace", metadata: { name: name } };
}

function serviceDoc(ns, name, port) {
  return {
    kind: "Service",
    metadata: { name: name, namespace: ns },
    spec: { ports: [{ name: "app", protocol: "TCP", port: port, targetPort: port }] },
  };
}

function sliceDoc(ns, svc, addresses, port) {
  return {
    kind: "EndpointSlice",
    metadata: {
      name: `${svc}-abc12`,
      namespace: ns,
      labels: { "kubernetes.io/service-name": svc },
    },
    addressType: "IPv4",
    endpoints: addresses.map((a) => ({ addresses: [a], conditions: { ready: true } })),
    ports: [{ name: "app", protocol: "TCP", port: port }],
  };
}

// --- multiregion ---

// The multi-region trace. Multi-backend routes are weighted splits on the
// HTTP carry; the draws below keep the source deployment's distribution.
const L4_SPEC = {
  backendRefsPerRoute: [
    [1, 1.0],
  ],
  endpointsPerService: [
    [4, 0.5],
    [5, 0.5],
  ],
  listenerPort: 3478,
  backendPort: 3478,
};

function multiregion(regions, seed) {
  const rng = mulberry32(seed === undefined ? 42 : seed);
  const world = {
    name: "multiregion",
    namespaces: [],
    // One shared GatewayClass, installed up front with the world: regions
    // come and go under it (the source deployment's actual shape).
    gatewayClasses: [fixtures.gatewayClass()],
    gateways: [],
    routes: [],
    services: [],
    slices: [],
    secrets: [],
  };
  for (let i = 1; i <= regions; i++) {
    const ns = `region-${i}`;
    world.namespaces.push(namespaceDoc(ns));
    world.gateways.push(
      fixtures.gateway({
        metadata: { name: `gw-${i}`, namespace: ns },
        spec: {
          gatewayClassName: "delta-gateway",
          listeners: [{ name: "turn", protocol: "HTTP", port: L4_SPEC.listenerPort }],
        },
      }),
    );
    const nBackends = draw(rng, L4_SPEC.backendRefsPerRoute);
    const backendRefs = [];
    for (let b = 1; b <= nBackends; b++) {
      const svc = `turn-svc-${i}${nBackends > 1 ? `-${b}` : ""}`;
      backendRefs.push({ name: svc, port: L4_SPEC.backendPort });
      const k = draw(rng, L4_SPEC.endpointsPerService);
      const addrs = [];
      for (let e = 0; e < k; e++) {
        addrs.push(ip4(i >> 8, i, e));
      }
      world.services.push(serviceDoc(ns, svc, L4_SPEC.backendPort));
      world.slices.push(sliceDoc(ns, svc, addrs, L4_SPEC.backendPort));
    }
    world.routes.push({
      apiVersion: `${GATEWAY_API_GROUP}/v1`,
      kind: "HTTPRoute",
      metadata: { name: `turn-route-${i}`, namespace: ns },
      spec: {
        parentRefs: [{ name: `gw-${i}`, sectionName: "turn" }],
        rules: [{ backendRefs: backendRefs }],
      },
    });
  }
  return world;
}

// --- sock-shop ---

// The public services of the Sock Shop reference application.
const SOCK_SHOP_SERVICES = [
  "front-end", "catalogue", "catalogue-db", "carts", "carts-db",
  "orders", "orders-db", "payment", "user", "user-db",
  "shipping", "queue-master", "rabbitmq",
];

const SOCK_SHOP_SPEC = {
  endpointsPerService: [
    [2, 0.5],
    [3, 0.5],
  ],
  listenerPort: 8080,
  backendPort: 80,
};

function sockShop(regions, seed) {
  const rng = mulberry32(seed === undefined ? 42 : seed);
  const world = {
    name: "sock-shop",
    namespaces: [],
    gatewayClasses: [fixtures.gatewayClass()],
    gateways: [],
    routes: [],
    services: [],
    slices: [],
    secrets: [],
  };
  for (let i = 1; i <= regions; i++) {
    const ns = `shop-${i}`;
    world.namespaces.push(namespaceDoc(ns));
    world.gateways.push(
      fixtures.gateway({
        metadata: { name: `gw-${i}`, namespace: ns },
        spec: {
          gatewayClassName: "delta-gateway",
          listeners: [{ name: "web", protocol: "HTTP", port: SOCK_SHOP_SPEC.listenerPort }],
        },
      }),
    );
    for (const svc of SOCK_SHOP_SERVICES) {
      const k = draw(rng, SOCK_SHOP_SPEC.endpointsPerService);
      const addrs = [];
      for (let e = 0; e < k; e++) {
        addrs.push(ip4(i, SOCK_SHOP_SERVICES.indexOf(svc), e));
      }
      world.services.push(serviceDoc(ns, svc, SOCK_SHOP_SPEC.backendPort));
      world.slices.push(sliceDoc(ns, svc, addrs, SOCK_SHOP_SPEC.backendPort));
      world.routes.push(
        fixtures.httpRoute({
          metadata: { name: `${svc}-route`, namespace: ns },
          spec: {
            parentRefs: [{ name: `gw-${i}`, sectionName: "web" }],
            hostnames: [`shop-${i}.example.com`],
            rules: [
              {
                matches: [{ path: { type: "PathPrefix", value: `/${svc}` } }],
                backendRefs: [{ name: svc, port: SOCK_SHOP_SPEC.backendPort }],
              },
            ],
          },
        }),
      );
    }
  }
  return world;
}

// "multiregion-l4" and "stunner-udp" are legacy aliases of the shape.
const SHAPES = {
  "multiregion": multiregion,
  "multiregion-l4": multiregion,
  "stunner-udp": multiregion,
  "sock-shop": sockShop,
};

// generate builds a world by shape name.
function generate(shape, regions, seed) {
  const fn = SHAPES[shape];
  if (!fn) {
    throw new Error(`unknown trace shape ${shape}; use ${Object.keys(SHAPES).join(", ")}`);
  }
  return fn(regions, seed);
}

module.exports = { generate, multiregion, sockShop, SHAPES };
