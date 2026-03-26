# dbsp JavaScript runtime

`js/` contains the `dbsp` CLI, a small JavaScript scripting environment for building and testing
DBSP circuits from the command line.

The maintained documentation lives under `doc/`. This README is only a short module-level guide.

## Read This First

For the canonical documentation, start with:

- [`doc/apps-dbsp-script.md`](/doc/apps-dbsp-script.md)
- [`doc/getting-started.md`](/doc/getting-started.md)
- `doc/concepts-programming-compilers-and-expressions.md`

## Build And Run

From this module:

```bash
make build
```

This builds `bin/dbsp`.

Run one of the bundled examples:

```bash
./bin/dbsp examples/join_project.js
./bin/dbsp -v examples/observer-demo.js
```

## What This Module Contains

- `main.go` starts the CLI and script VM.
- `vm.go` hosts the Goja runtime and shared DBSP runtime.
- `sql.go`, `aggregate.go`, `circuit.go`, `producer.go`, and `consumer.go` expose the scripting API.
- `examples/` contains the scripts referenced by the docs.

For the full API reference, Kubernetes connector examples, observer examples, and execution model,
use [`doc/apps-dbsp-script.md`](/doc/apps-dbsp-script.md).
