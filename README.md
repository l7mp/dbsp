[![CI](https://github.com/l7mp/dbsp/actions/workflows/ci.yml/badge.svg)](https://github.com/l7mp/dbsp/actions/workflows/ci.yml)
[![Engine Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/engine.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/engine)
[![JS Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/js.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/js)
[![Δ-controller Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/dcontroller.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/dcontroller)
[![Engine Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dbsp/engine)](https://goreportcard.com/report/github.com/l7mp/dbsp/engine)
[![JS Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dbsp/js)](https://goreportcard.com/report/github.com/l7mp/dbsp/js)
[![Δ-controller Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dbsp/dcontroller)](https://goreportcard.com/report/github.com/l7mp/dbsp/dcontroller)

# DBSP

DBSP is a Go workspace for incremental computation. The repository contains the core circuit engine,
a JavaScript scripting environment for prototyping and experiments, Kubernetes connectors, and
Δ-controller, a declarative controller framework built on top of the same runtime.

The maintained documentation lives under `doc/`. This README is a short repository-level guide.

## Read This First

For the canonical documentation, start with:

- [`doc/README.md`](/doc/README.md)
- [`doc/what-is-dbsp.md`](/doc/what-is-dbsp.md)
- [`doc/getting-started.md`](/doc/getting-started.md)
- [`doc/apps-dbsp-script.md`](/doc/apps-dbsp-script.md)
- [`doc/apps-dctl-overview.md`](/doc/apps-dctl-overview.md)

## Workspace Modules

- `engine/` contains the DBSP circuit model, operators, compilers, runtime, and Z-set data model.
- `js/` contains the `dbsp` JavaScript scripting runtime.
- `dcontroller/` contains the Δ-controller module.
- `connectors/kubernetes/` and `connectors/misc/` provide runtime integrations.

For module-specific entry points, use:

- [`engine/README.md`](/engine/README.md)
- [`js/README.md`](/js/README.md)
- [`dcontroller/README.md`](/dcontroller/README.md)

## Build And Test

From the workspace root:

```bash
make build
make test-fast
make test
```

The workspace is managed by `go.work`, so module-local builds also work from `engine/`, `js/`, and
`dcontroller/`.

## Applications

The current user-facing application layers documented in this repository are:

- [`doc/apps-dbsp-script.md`](/doc/apps-dbsp-script.md) for the `dbsp` JavaScript runtime.
- [`doc/apps-dctl-overview.md`](/doc/apps-dctl-overview.md) for Δ-controller.
