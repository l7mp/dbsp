# Getting Started

If you want to try DBSP quickly, the shortest path is the `dbsp` JavaScript runner in
`js/`. It lets you execute bundled examples and small local scripts without building the
whole workspace first.

## Install the CLI with Go

If you already have a recent Go toolchain, install the script runner directly:

```bash
go install github.com/l7mp/dbsp/js@latest
```

This builds the `dbsp` binary and places it in your Go bin directory, usually
`$GOPATH/bin` or `$HOME/go/bin`. Make sure that directory is on your `PATH`.

Check that the binary is available:

```bash
dbsp --help
```

The installed CLI runs DBSP JavaScript programs. If you want an example to start from,
clone the repository as well so you can use the scripts in `js/examples/`:

```bash
git clone https://github.com/l7mp/dbsp.git
cd dbsp/js
dbsp examples/join_project.js
```

The `join_project.js` example shows a small end-to-end flow: define tables, compile a
query into a circuit, incrementalize it, and then feed updates into the runtime.

## Build from a local checkout

Building from source is the better path if you want to explore the repository, run tests,
or modify the implementation.

Clone the repository and enter the workspace root:

```bash
git clone https://github.com/l7mp/dbsp.git
cd dbsp
```

This repository uses a Go workspace (`go.work`) with several modules, including:

- `engine/` for the core DBSP library,
- `js/` for the `dbsp` CLI and script runner,
- `connectors/*` for runtime integrations, and
- `dcontroller/` for controller-oriented applications.

### Build

From the repository root, run:

```bash
make build
```

This performs a compile check for the workspace modules, builds the JavaScript runner,
and builds the controller binaries and examples.

### Build only the CLI

If you only want the script runner, build the `js/` module directly:

```bash
cd js
make build
```

This writes the binary to `js/bin/dbsp`.

Run one of the bundled examples:

```bash
./bin/dbsp examples/join_project.js
```

You can also try:

```bash
./bin/dbsp examples/observer-demo.js
```

## Run tests

For a quick confidence check from the workspace root, run:

```bash
make test-fast
```

To run the full test suite:

```bash
make test
```

If you are working only on the script runner, run tests just for that module:

```bash
cd js
make test
```
