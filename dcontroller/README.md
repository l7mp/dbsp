[![Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dbsp/dcontroller)](https://goreportcard.com/report/github.com/l7mp/dbsp/dcontroller)
[![Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/dcontroller.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/dcontroller)
[![CI](https://github.com/l7mp/dbsp/actions/workflows/ci.yml/badge.svg)](https://github.com/l7mp/dbsp/actions/workflows/ci.yml)

# Δ-controller

Δ-controller is the Kubernetes application layer in this repository. It applies the generic DBSP
runtime to Kubernetes resources so that controllers can be written as declarative pipelines over
object deltas instead of imperative reconciliation loops.

The maintained documentation lives under `doc/`. This README is only a short module-level guide.

## Read This First

For the canonical documentation, start with:

- [`doc/apps-dctl-overview.md`](/doc/apps-dctl-overview.md)
- [`doc/apps-dctl-getting-started.md`](/doc/apps-dctl-getting-started.md)
- [`doc/apps-dctl-sources-targets-pipeline.md`](/doc/apps-dctl-sources-targets-pipeline.md)
- [`doc/apps-dctl-extension-api-server.md`](/doc/apps-dctl-extension-api-server.md)

If you are new to the broader project, also read:

- [`doc/what-is-dbsp.md`](/doc/what-is-dbsp.md)
- [`doc/getting-started.md`](/doc/getting-started.md)
- [`doc/README.md`](/doc/README.md)

## What This Module Contains

- `api/` defines the `Operator` custom resource types.
- `controller/` compiles controller specs into running DBSP-backed pipelines.
- `operator/` manages operator lifecycle and shared runtime wiring.
- `examples/` contains the manifests and example programs referenced by the docs.

In the current implementation, an `Operator` contains one or more controllers. Each controller
declares `sources`, a `pipeline`, and `targets`. Sources and targets can be native Kubernetes
resources or local in-memory views.

## Build And Run

From this module:

```bash
make build
```

This builds `bin/dctl`.

For local development against your current Kubernetes context:

```bash
./bin/dctl start --http --disable-authentication
```

For the full current workflow, including TLS and view access, use
[`doc/apps-dctl-getting-started.md`](/doc/apps-dctl-getting-started.md).

## Examples

The example directories are:

- [`examples/configmap-deployment-controller/`](/dcontroller/examples/configmap-deployment-controller/)
- [`examples/service-health-monitor/`](/dcontroller/examples/service-health-monitor/)
- [`examples/endpointslice-controller/`](/dcontroller/examples/endpointslice-controller/)

The maintained walkthroughs for those examples are:

- [`doc/apps-dctl-tutorial-configmap-deployment.md`](/doc/apps-dctl-tutorial-configmap-deployment.md)
- [`doc/apps-dctl-tutorial-service-health.md`](/doc/apps-dctl-tutorial-service-health.md)
- [`doc/apps-dctl-tutorial-endpointslice.md`](/doc/apps-dctl-tutorial-endpointslice.md)
