[![Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dbsp/dcontroller)](https://goreportcard.com/report/github.com/l7mp/dbsp/dcontroller)
[![Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/dcontroller.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/dcontroller)
[![CI](https://github.com/l7mp/dbsp/actions/workflows/ci.yml/badge.svg)](https://github.com/l7mp/dbsp/actions/workflows/ci.yml)

# Δ-controller

Δ-controller is the Kubernetes application layer in this repository. It applies the generic DBSP
runtime to Kubernetes resources so that controllers can be written as declarative pipelines over
object deltas instead of imperative reconciliation loops.

The maintained documentation lives under [`doc/`](/doc/) and example controllers are in
[`examples/`](examples/). This README is only a short module-level guide.

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
