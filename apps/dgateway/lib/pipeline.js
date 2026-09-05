// The delta-gateway pipeline: three circuits layered around curated views.
//
//   INPUT layer (one circuit): the watched Kubernetes objects are folded
//   into a small set of curated views - the controller's internal state,
//   one per major CRD - each built top to bottom by its own module
//   (branches):
//
//     GatewayClass ---> gatewayclass.js -> GatewayClassView
//     Gateway, Secret > gateway.js -----> GatewayView   (+ the attachment
//     HTTPRoute,       > httproute.js ---> RouteView       facet from
//       Namespace                                          httproute.js)
//     Service, ------> backend.js ------> BackendView
//       EndpointSlice,
//       BackendTLSPolicy
//     GatewayClass,    > observed.js ----> *StatusObserved (the observed
//       Gateway,                            statuses the Reconciler pairs
//       HTTPRoute                           with the status outputs)
//
//   OUTPUT layers (one circuit per realm), rendering views only:
//
//     statusBranches of gatewayclass.js, gateway.js, httproute.js:
//                  views -> status documents, one writer per CRD, in
//                  the CRD's own module
//     xds.js:      views -> Xds{Listeners,RouteConfigurations,Clusters,
//                  Endpoints} (Envoy protojson, bound to the operator's
//                  xDS egress server by the loader)
//
// Row conventions, all modules: a field named after an object holds the
// object - {name, namespace} references for gateway, route (plus its
// generation), service and secret, the curated listener object from
// gateway.js - and a bare name carries the Name suffix (listenerName,
// rdsName, clusterName, portName). Intermediate topics are camelCase
// nouns; views, inputs and outputs are CamelCase. Only views and
// Kubernetes documents carry a metadata identity.

const {
  CONTROLLER_NAME,
  OPERATOR,
  RESOURCES,
  XDS_GROUP,
  TOPICS,
} = require("./config.js");

// @selectorMatches (Kubernetes LabelSelector semantics: matchLabels plus
// matchExpressions) is implemented in Go by the Kubernetes connector; apps
// opt in per operator. Registration is init-phase only and needs no cluster
// or running connector runtime.
kubernetes.expression.register("@selectorMatches");
const gatewayclass = require("./pipeline/gatewayclass.js");
const gateway = require("./pipeline/gateway.js");
const httproute = require("./pipeline/httproute.js");
const backend = require("./pipeline/backend.js");
const observed = require("./pipeline/observed.js");
const xdsOutput = require("./pipeline/xds.js");

// buildPrograms assembles the three layer programs.
function buildPrograms(options = {}) {
  const params = {
    controllerName: options.controllerName || CONTROLLER_NAME,
    // A loopback pool (e.g. "127.0.0.0/16") for local single-host runs:
    // every gateway gets its own deterministic data-plane address, reported
    // in the Gateway status and bound by its Envoy listeners. Without a pool
    // no addresses are reported and listeners bind 0.0.0.0.
    addressPool: options.addressPool || null,
  };

  return {
    params,
    input: [
      ...gatewayclass.branches(params),
      ...gateway.branches(params),
      ...httproute.branches(params),
      ...backend.branches(params),
      ...observed.branches(params),
    ],
    outputK8s: [
      ...gatewayclass.statusBranches(params),
      ...gateway.statusBranches(params),
      ...httproute.statusBranches(params),
    ],
    outputXds: xdsOutput.branches(params),
  };
}

// The operator's streams. Views and observed statuses are internal
// wires: circuit outputs consumed by circuit inputs, no binding at all.
const VIEWS = ["GatewayClassView", "GatewayView", "RouteView", "BackendView"];
const OBSERVED = ["GatewayClassStatusObserved", "GatewayStatusObserved", "HTTPRouteStatusObserved"];
// (observed stream, status stream) pairs: the closed loop's plant feedback.
const STATUS_PAIRS = [
  ["GatewayClassStatusObserved", "GatewayClassStatus"],
  ["GatewayStatusObserved", "GatewayStatus"],
  ["HTTPRouteStatusObserved", "HTTPRouteStatus"],
];

// buildOperatorSpec assembles the serialized runtime: three circuits
// meeting on the internal view and observed streams. With bindings
// "kubernetes" the sources watch the cluster and the statuses go through
// Patchers (the deployable spec); with bindings "topics" (the default,
// the self-contained test mode) there are no Kubernetes bindings at all:
// the harness drives the input streams and reads the status streams
// through the runtime handle. The xDS targets are real in both modes:
// the egress server must be started under the operator's name before
// loading.
function buildOperatorSpec(options = {}) {
  const k8sBindings = options.bindings === "kubernetes";

  // State-of-the-world mode runs the same programs with empty transform
  // chains: the engine commits them with the snapshot adapters
  // (integrators on the inputs, the program recomputing over the full
  // state, differentiation on the outputs), so the bus still carries
  // deltas and the connectors are the same plain ones. The loop
  // transforms are edge-world constructions and stay off in sotw mode.
  const sotw = Boolean(options.sotw);
  const incTransforms = sotw ? [] : [{ name: "Incrementalizer" }];
  const k8sTransforms = sotw ? [] : [{ name: "Incrementalizer" }];
  if (options.reconcile) {
    // Close the loop on everything we can observe: the status outputs
    // emit the outstanding correction U = ∫(δD - δY_U) - re-emitted every
    // step until the observed state confirms it - so unchanged statuses
    // cost no writes, external tampering heals, and lost writes retry by
    // construction. Controller mode only: the loop needs the watch
    // feedback (the self-contained tests have no plant, so U would never
    // quiesce there); the xDS circuit stays open loop either way - Envoy
    // state is not observed (yet).
    k8sTransforms.push({ name: "Reconciler", pairs: STATUS_PAIRS });
  } else if (options.smith) {
    // The dead-time compensated loop: same closed-loop pairs, but the
    // DualRateSmith actuates every correction exactly once and retires
    // the loop's own watch echoes through a wall-clock window - commands
    // enter on the event clock and expire k ticks later, so the window
    // is k seconds at the default tick and event floods never age it.
    // Controller mode only, like the Reconciler. k is the compensation
    // window in ticks and must be given.
    const k = Number(options.smithK);
    if (!Number.isInteger(k) || k < 1) {
      throw new Error(`pipeline: smith mode needs an integer window k >= 1 ticks, got ${options.smithK}`);
    }
    k8sTransforms.push({ name: "DualRateSmith", pairs: STATUS_PAIRS, k, tick: "Tick" });
  }

  const programs = buildPrograms(options);
  const STATUSES = ["GatewayClassStatus", "GatewayStatus", "HTTPRouteStatus"];
  const XDS_STREAMS = ["XdsListeners", "XdsRouteConfigurations", "XdsClusters", "XdsEndpoints"];
  // The window read-out clock: a misc Tick source in pulse mode (one
  // empty document asserted per period) feeding the tick input the
  // DualRateSmith injects. One tick per operator; smith mode only, and
  // the default cadence is one second. options.tick is a Go duration
  // string.
  const tickSources = options.smith
    ? [{
        apiGroup: "misc.connector.dcontroller.io",
        kind: "Timer",
        as: "Tick",
        type: "Tick",
        parameters: { period: options.tick || "1s", pulse: true },
      }]
    : [];
  return {
    sources: [...(k8sBindings ? Object.values(RESOURCES) : []), ...tickSources],
    circuits: [
      {
        name: "input",
        inputs: Object.keys(TOPICS.inputs).map((k) => TOPICS.inputs[k]),
        outputs: [...VIEWS, ...OBSERVED],
        pipeline: programs.input,
        transforms: incTransforms,
      },
      {
        name: "k8s",
        // The Tick input is the window read-out clock the DualRateSmith
        // wires into its gates; no branch consumes it. Declared here so
        // the loader can bind the tick source to the circuit.
        inputs: [...VIEWS, ...OBSERVED, ...(options.smith ? ["Tick"] : [])],
        outputs: STATUSES,
        pipeline: programs.outputK8s,
        transforms: k8sTransforms,
      },
      {
        name: "xds",
        inputs: VIEWS,
        outputs: XDS_STREAMS,
        pipeline: programs.outputXds,
        transforms: incTransforms,
      },
    ],
    targets: [
      ...(k8sBindings
        ? [
            { ...RESOURCES.gatewayClass, type: "Patcher", as: "GatewayClassStatus" },
            { ...RESOURCES.gateway, type: "Patcher", as: "GatewayStatus" },
            { ...RESOURCES.httpRoute, type: "Patcher", as: "HTTPRouteStatus" },
          ]
        : []),
      { apiGroup: XDS_GROUP, kind: "Listener", as: "XdsListeners" },
      { apiGroup: XDS_GROUP, kind: "RouteConfiguration", as: "XdsRouteConfigurations" },
      { apiGroup: XDS_GROUP, kind: "Cluster", as: "XdsClusters" },
      { apiGroup: XDS_GROUP, kind: "ClusterLoadAssignment", as: "XdsEndpoints" },
    ],
  };
}

// Pipeline is one loaded operator: the serialized runtime fed through
// runtime.create. The circuits are named plainly ("input", "k8s",
// "xds") and every stream is a topic of the same name in the operator's
// private runtime (this.topics), so the controller's internal state is
// introspectable live with a handle.subscribe().
class Pipeline {
  constructor(options = {}) {
    this.stem = options.name || OPERATOR;
    this.spec = buildOperatorSpec(options);
    this.handle = runtime.create(this.stem, this.spec);
    this.handle.start();
    this.topics = TOPICS;
  }

  // observe attaches a debug observer to one layer ("input", "k8s" or
  // "xds"): fn receives every event the layer's circuit processes.
  observe(layer, fn) {
    if (!["input", "k8s", "xds"].includes(layer)) {
      throw new Error(`pipeline.observe: unknown layer ${layer}; use input, k8s or xds`);
    }
    this.handle.observe(layer, fn);
  }

  close() {
    this.handle.close();
  }
}

// compilePipeline loads the operator and returns the Pipeline instance.
function compilePipeline(options = {}) {
  return new Pipeline(options);
}

module.exports = {
  Pipeline,
  buildPrograms,
  buildOperatorSpec,
  compilePipeline,
};
