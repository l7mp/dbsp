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
//                  Endpoints} (Envoy protojson, served as is by
//                  lib/xdsmap.js)
//
// The circuits communicate over the (retained) view topics, so the views
// are introspectable live and each output layer can be transformed
// independently - the k8s layer takes the desired-state Reconciler, the
// unobservable xDS layer stays open loop.
//
// Row conventions, all modules: a field named after an object holds the
// object - {name, namespace} references for gateway, route (plus its
// generation), service and secret, the curated listener object from
// gateway.js - and a bare name carries the Name suffix (listenerName,
// rdsName, clusterName, portName). Intermediate topics are camelCase
// nouns; views, inputs and outputs are CamelCase. Only views and
// Kubernetes documents carry a metadata identity.

const { CONTROLLER_NAME, TOPICS } = require("./config.js");

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

// Pipeline is one controller's compiled circuit stack: the input layer and
// the two output layers, bound to the configured topics. One instance per
// controller; the circuit handles are exposed as .input/.k8s/.xds and
// debug observers attach per layer via .observe().
class Pipeline {
  constructor(options = {}) {
    const compiled = compileCircuits(options);
    this.stem = compiled.stem;
    this.topics = compiled.topics;
    this.input = compiled.input;
    this.k8s = compiled.k8s;
    this.xds = compiled.xds;
  }

  // observe attaches a debug observer to one layer ("input", "k8s" or
  // "xds"): fn receives every event the layer's circuit processes. Any
  // internal state worth inspecting beyond that should be (and mostly is)
  // a pipeline output - the view topics are ordinary retained topics, so
  // subscribe() to this.topics.views.* sees the controller's internal
  // state live.
  observe(layer, fn) {
    const names = { input: `${this.stem}-input`, k8s: `${this.stem}-k8s`, xds: `${this.stem}-xds` };
    const name = names[layer];
    if (!name) {
      throw new Error(`pipeline.observe: unknown layer ${layer}; use input, k8s or xds`);
    }
    runtime.observe(name, fn);
  }
}

// compilePipeline compiles the three circuits, incrementalizes them, and
// binds them to the configured topics. Returns the Pipeline instance.
function compilePipeline(options = {}) {
  return new Pipeline(options);
}

function compileCircuits(options = {}) {
  const topics = options.topics || TOPICS;
  const programs = buildPrograms(options);
  const stem = options.name || "delta-gateway";

  const views = [
    { name: topics.views.gatewayClass, logical: "GatewayClassView" },
    { name: topics.views.gateway, logical: "GatewayView" },
    { name: topics.views.route, logical: "RouteView" },
    { name: topics.views.backend, logical: "BackendView" },
  ];
  // Observed statuses (the π_U projection curated by the input layer) and
  // the Reconciler pairs matching them to the k8s status outputs.
  const observed = [
    { name: topics.observed.gatewayClass, logical: "GatewayClassStatusObserved" },
    { name: topics.observed.gateway, logical: "GatewayStatusObserved" },
    { name: topics.observed.httpRoute, logical: "HTTPRouteStatusObserved" },
  ];
  // Reconciler pairs are (input, output) circuit nodes, named by topic.
  const reconcilerPairs = [
    [topics.observed.gatewayClass, topics.status.gatewayClass],
    [topics.observed.gateway, topics.status.gateway],
    [topics.observed.httpRoute, topics.status.httpRoute],
  ];

  const input = aggregate.compile(programs.input, {
    inputs: [
      { name: topics.inputs.gatewayClass, logical: "GatewayClass" },
      { name: topics.inputs.gateway, logical: "Gateway" },
      { name: topics.inputs.route, logical: "HTTPRoute" },
      { name: topics.inputs.service, logical: "Service" },
      { name: topics.inputs.endpointSlice, logical: "EndpointSlice" },
      { name: topics.inputs.secret, logical: "Secret" },
      { name: topics.inputs.backendTLSPolicy, logical: "BackendTLSPolicy" },
      { name: topics.inputs.namespace, logical: "Namespace" },
    ],
    outputs: [...views, ...observed],
    name: `${stem}-input`,
  });
  // Transform lists: the engine applies a list in canonical order as one
  // atomic step, so the pipeline states WHAT each layer is, not the order.
  if (options.sotw) {
    // State-of-the-world mode needs full-snapshot ingest and
    // state-of-the-world writes end to end, which the connector stack
    // does not provide: the mode is rejected rather than approximated
    // with deltas.
    throw new Error("pipeline: sotw mode is not available; the state-of-the-world stack is not wired");
  }
  const layerTransforms = () => [{ name: "Incrementalizer" }];

  input.transform(layerTransforms());
  input.commit();

  const k8s = aggregate.compile(programs.outputK8s, {
    inputs: [...views, ...observed],
    outputs: [
      { name: topics.status.gatewayClass, logical: "GatewayClassStatus" },
      { name: topics.status.gateway, logical: "GatewayStatus" },
      { name: topics.status.httpRoute, logical: "HTTPRouteStatus" },
    ],
    name: `${stem}-k8s`,
  });
  const k8sTransforms = layerTransforms();
  if (options.reconcile) {
    // Close the loop on everything we can observe: the status outputs
    // emit the outstanding correction U = ∫(δD - δY_U) - re-emitted every
    // step until the observed state confirms it - so unchanged statuses
    // cost no writes, external tampering heals, and lost writes retry by
    // construction. Controller mode only: the loop needs the watch
    // feedback (the self-contained tests have no plant, so U would never
    // quiesce there); the xDS circuit stays open loop either way - Envoy
    // state is not observed (yet).
    k8sTransforms.push({ name: "Reconciler", pairs: reconcilerPairs });
  } else if (options.smith) {
    // The dead-time compensated loop: same closed-loop pairs, but the
    // SmithPredictor emits U_out = (δD - δY_U) + (δY_U ⋉ z⁻¹U) - every
    // correction actuated exactly once, the loop's own watch echoes
    // retired by content, however late the apiserver reflects a write.
    // Controller mode only, like the Reconciler. The dead time k is the
    // loop's known feedback delay in circuit steps and must be given.
    const k = Number(options.smithK);
    if (!Number.isInteger(k) || k < 2) {
      throw new Error(`pipeline: smith mode needs an integer dead time k >= 2, got ${options.smithK}`);
    }
    k8sTransforms.push({ name: "SmithPredictor", pairs: reconcilerPairs, k });
  }
  k8s.transform(k8sTransforms);
  k8s.commit();

  const xds = aggregate.compile(programs.outputXds, {
    inputs: views,
    outputs: [
      { name: topics.xds.listeners, logical: "XdsListeners" },
      { name: topics.xds.routes, logical: "XdsRouteConfigurations" },
      { name: topics.xds.clusters, logical: "XdsClusters" },
      { name: topics.xds.endpoints, logical: "XdsEndpoints" },
    ],
    name: `${stem}-xds`,
  });
  xds.transform(layerTransforms());
  xds.commit();

  return { stem, topics, input, k8s, xds };
}

module.exports = {
  Pipeline,
  buildPrograms,
  compilePipeline,
};
