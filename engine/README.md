# DBSP Engine

`engine` is the core incremental computation module of this monorepo.

Module path:

```text
github.com/l7mp/dbsp/engine
```

It contains the circuit model, operators, compiler, executor/runtime, and Z-set
data structures used by higher-level modules.

## Layout

- `circuit`: Graph model for DBSP circuits.
- `operator`: Linear, bilinear, and nonlinear operators.
- `compiler`: SQL and aggregation compilers.
- `executor`: Step execution over compiled circuits.
- `runtime`: Runtime wiring for producers and consumers.
- `transform`: Incrementalization and rewrite logic.
- `zset`: Weighted multiset implementation.
- `datamodel`, `expression`: Document and expression layers.

## Quick Start

```bash
go test ./...
```

From repository root:

```bash
GOWORK=off go test ./...
```

## Related Modules

- `github.com/l7mp/dbsp/connectors/kubernetes`
- `github.com/l7mp/dbsp/connectors/misc`
- `github.com/l7mp/dbsp/shell`
- `github.com/l7mp/dbsp/dcontroller`
