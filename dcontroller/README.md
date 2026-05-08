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

Build the DBSP JS runtime from the workspace root:

```bash
make -C js build
```

This builds `js/bin/dbsp`.

For local development against your current Kubernetes context:

```bash
./js/bin/dbsp dcontroller/dcontroller.js
```

For one-off utility actions (for example kubeconfig generation), use inline
evaluation:

```bash
./js/bin/dbsp -e 'const cfg = kubernetes.runtime.config({apiServer:{addr:"localhost",port:8443,http:true}}); const yaml = cfg.generateKubeConfig({user:"dev",namespaces:["*"],keyFile:"apiserver.key",serverAddress:"localhost:8443",http:true}); require("fs").writeFileSync("/tmp/dcontroller.config", yaml);'
```

For the full current workflow, including TLS and view access, use
[`doc/apps-dctl-getting-started.md`](/doc/apps-dctl-getting-started.md).

## License

Copyright 2026 by its authors. Some rights reserved. See [AUTHORS](/AUTHORS).

MIT License - see [LICENSE](/LICENSE) for full text.
