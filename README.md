[![CI](https://github.com/l7mp/dbsp/actions/workflows/ci.yml/badge.svg)](https://github.com/l7mp/dbsp/actions/workflows/ci.yml)<br>
[![Engine Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/engine.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/engine)
[![JS Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/js.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/js)
[![Δ-controller Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/dcontroller.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/dcontroller)

# DBSP: A runtime for incremental declarative controllers 

DBSP is a runtime for **incremental control loops**. Describe the controller as a declarative
pipeline (or as SQL), and the DBSP runtime applies your control logic to the system as inputs
change, computing only the delta instead of recomputing the entire system state from scratch.

This repository contains a clean-slate Go implementation of the DBSP incremental computation
engine, a JavaScript runtime, Kubernetes connectors, and Δ-controller, a declarative Kubernetes
operator framework, the first application built on top. The engine itself is general; more
applications are planned.

Long-form documentation lives under [`doc/`](/doc/README.md).

## Description

Modern infrastructure runs on control loops. [Kubernetes operators](https://book.kubebuilder.io/)
and [policy engines](https://kyverno.io/), the 5G/6G mobile core, data center fabrics and SDNs, all
observe state, derive the desired state, and drive the system toward it. DBSP is a unified runtime
to handle any control loop of this kind in a single framework.

- **Work proportional to the change, not the entire state.** The standard Kubernetes operator
  pattern recomputes the entire desired state on every event. At small scale this works fine, but
  it breaks at massive scale. DBSP pipelines touch only the records affected by a delta, not the
  entire state.

- **Provably stable incremental control loops.** Δ-controller automatically rewrites controllers
  into an incremental form using the [Database Stream
  Processing](https://doi.org/10.1007/s00778-025-00922-y) framework and adds a theoretically sound
  incremental desired-state reconciler on top. This makes sure the system state stabilizes at the
  desired state even in the face of adversarial modifications to the system state.

- **An JavaScript runtime for quick prototyping, a Kubernetes CRD to fire up controllers
  dynamically, and an embeddable Go library for production.** Choose the right form factor for your
  needs; each implementation wraps the same MongoDB-inspired aggregation language and DBSP
  incremental computation engine. JavaScript is number one citizen: Δ-controller itself is fully
  written using the JS runtime.

## Getting started

### Installation

if you want to use just Δ-controller via a injecting controllers as YAML files:

```bash
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm install dcontroller dcontroller/dcontroller --set apiServer.mode=production --set apiServer.service.type=LoadBalancer
```

This will start the embedded API server in hardened more and expose it via a LoadBalancer Service
on port 8443. This makes it possible to inspect and load Δ-controller's internal objects, called
"views", via a standard `kubectl` client.

To use the full JS suite:

```bash
make generate manifests test build
```

### A Kubernetes controller in JavaScript

The below script implements a simple Kubernetes Endpoints controller: for every `Service`, find the
`Pods` whose labels match the service's selector, and produce an `Endpoints` object listing the Pod
IPs. 

```js
"use strict";

// Wire up the Kubernetes runtime once. Use the default KUBECONFIG.
kubernetes.runtime.start({});

const pipeline = [
  // 1. Join every Pod with every Service based in the "app" label.
  {"@join": {"@eq": [
    "$.Pod.metadata.labels.app",
    "$.Service.spec.selector.app"
  ]}},

  // 2. Keep only running Pods.
  {"@select": {"@eq": ["$.Pod.status.phase", "Running"]}},

  // 3. Per each Service-Pod pair, emit a (service identity, pod IP) row.
  {"@project": {
    "id":    {"name": "$.Service.metadata.name",
              "namespace": "$.Service.metadata.namespace"},
    "podIP": "$.Pod.status.podIP"
  }},

  // 4. Group all Pod IPs under each Service.
  {"@groupBy": ["$.id", "$.podIP"]},

  // 5. Shape the result into an Endpoints view object.
  {"@project": {
    "apiVersion": "endpoints.view.dcontroller.io/v1alpha1",
    "kind":       "Endpoints",
    "metadata":   "$.key",
    "addresses":  "$.values"
  }}
];

// Compile the pipeline into a DBSP circuit.
const c = aggregate.compile(pipeline, {
  inputs:  ["Pods", "Service"],
  outputs: ["Endpoints"],
});

// Incrementalize the circuit, regularize it, and close the control loop.
// After this transform the circuit consumes deltas, emits deltas, and
// applies the diff between desired and observed state on every step.
c.transform("Incrementalizer").validate();

// Wire the circuit to Kubernetes: watch Pods and Services, write Endpoints.
kubernetes.watch ("Pod",       {gvk: "v1/Pod"});
kubernetes.watch ("Service",   {gvk: "v1/Service"});
kubernetes.update("Endpoints", {gvk: "endpoints.view.dcontroller.io/v1alpha1/Endpoints"});
```

To run it against a cluster:

```bash
./js/bin/dbsp ./examples/endpoints.js
```

There is no reconcile loop, no work queue, no event handler, just a pipeline written as a
MongoDB-like aggregation query wrapped with a couple of Kubernetes watches to feed it with input
and updaters that apply the results back to Kubernetes.  When a Pod transitions to `Running` state,
the Kubernetes Pod watch posts an update to the "pods" topic the controller subscribes to, which
triggers a new pipeline evaluation. The pipeline processes only that single updated Pod through the
JOIN, regroups exactly the affected Service, and emits a single delta on the `Endpoints` output to
reconcile the new state. When a Service is deleted, the matching Pods drop out of the join, and the
corresponding Endpoints view is deleted in the same pass. The work done is proportional to the
change. Add a watch on the created Endpoints objects and the controller is now provably safe
against adversaries tinkering with Kubernetes state.

### A Kubernetes controller as a CRD

The same controller can also be written as a Kubernetes Custom Resource and applied with `kubectl`:

```yaml
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: endpoints
spec:
  controllers:
  - name: endpoints-controller
    sources:
    - {apiGroup: "", kind: Pod}
    - {apiGroup: "", kind: Service}
    targets:
    - {apiGroup: view.dcontroller.io, kind: Endpoints}
    pipeline:  # same pipeline as above, expressed as YAML
    - "@join":    {"@eq": ["$.Pod.metadata.labels.app",
                           "$.Service.spec.selector.app"]}
    - "@select":  {"@eq": ["$.Pod.status.phase", "Running"]}
    - "@project": {id: {name: "$.Service.metadata.name",
                        namespace: "$.Service.metadata.namespace"},
                   podIP: "$.Pod.status.podIP"}
    - "@groupBy": ["$.id", "$.podIP"]
    - "@project": {apiVersion: "endpoints.view.dcontroller.io/v1alpha1",
                   kind: "Endpoints",
                   metadata: "$.key",
                   addresses: "$.values"}
```

Save this to `endpoints-controller.yaml` and inject it via `kubectl`:

```bash
kubectl apply -f endpoints-controller.yaml
```

### Views

The `Endpoints` object above is a *view*, a regular Kubernetes resource served by Δ-controller's
embedded API server. We use views here only to avoid having to install an Endpoints CRD to
Kubernetes. 

Inspect the Endpoints generated by the controller:

```bash
export KUBECONFIG=/tmp/dcontroller.config
dbsp apiserver/generate_config --user=dev --namespaces='*' --profile=admin \
    --tls-key-file=apiserver.key --server-address=localhost:8443 --http > "$KUBECONFIG"
kubectl get endpoints.view.dcontroller.io.Endpoints
```

## CAVEATS

- This is alpha quality software. Use it at your own risk. 

- The embedded DBSP incremental computation engine has not been optimized at all. For anything
  resource-intensive, use a [high-performance DBSP
  implementation](https://github.com/feldera/feldera).

## License

Copyright 2026 by its authors. See [AUTHORS](/AUTHORS).

MIT License. See [LICENSE](/LICENSE).
