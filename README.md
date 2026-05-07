[![CI](https://github.com/l7mp/dbsp/actions/workflows/ci.yml/badge.svg)](https://github.com/l7mp/dbsp/actions/workflows/ci.yml)<br>
[![Engine Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/engine.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/engine)
[![JS Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/js.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/js)
[![Δ-controller Go Reference](https://pkg.go.dev/badge/github.com/l7mp/dbsp/dcontroller.svg)](https://pkg.go.dev/github.com/l7mp/dbsp/dcontroller)<br>
[![Engine Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dbsp/engine)](https://goreportcard.com/report/github.com/l7mp/dbsp/engine)
[![JS Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dbsp/js)](https://goreportcard.com/report/github.com/l7mp/dbsp/js)
[![Δ-controller Go Report Card](https://goreportcard.com/badge/github.com/l7mp/dbsp/dcontroller)](https://goreportcard.com/report/github.com/l7mp/dbsp/dcontroller)

# DBSP

DBSP is a Go workspace for incremental computation. The repository contains the core circuit engine,
a JavaScript scripting environment for prototyping and experiments, Kubernetes connectors, and
Δ-controller, a declarative controller framework built on top of the same runtime.

The maintained documentation lives under [`doc/`](/doc/README.md). This README is a short repository-level guide.

## Modules

- [`engine/`](/engine/README.md): DBSP circuit model, operators, compilers, runtime, and Z-set data model.
- [`js/`](/js/README.md): the `dbsp` JavaScript scripting runtime.
- [`dcontroller/`](/dcontroller/README.md): Δ-controller.
- [`connectors/kubernetes/`](connectors/kubernetes/) and [`connectors/misc/`](connectors/misc/): runtime integrations.

## Build And Test

From the workspace root:

```bash
make build
make test-fast
make test
```

## Applications

The current user-facing application layers documented in this repository are:

- [`doc/apps-dbsp-script.md`](/doc/apps-dbsp-script.md) for the `dbsp` JavaScript runtime.
- [`doc/apps-dctl-overview.md`](/doc/apps-dctl-overview.md) for Δ-controller.

## License

Copyright 2026 by its authors. Some rights reserved. See [AUTHORS](/AUTHORS).

MIT License - see [LICENSE](/LICENSE) for full text.
