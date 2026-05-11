# dbsp JavaScript runtime

`js/` contains the `dbsp` CLI, a small JavaScript scripting environment for building and testing
DBSP circuits from the command line.

The maintained documentation lives under [`doc/`](/doc/). This README is only a short module-level
guide.

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

## Script arguments

The CLI forwards positional arguments after the script path into JavaScript as `process.argv`.

```bash
./bin/dbsp examples/gwapi/index.js test gwclass
```

In JS, this is visible as:

- `process.argv[0]`: dbsp binary path
- `process.argv[1]`: script path
- `process.argv[2...]`: user script arguments

## Standard JS modules

The runtime supports loading standard JavaScript modules from:

- `js/lib`

It also supports loading vendored third-party JavaScript modules from:

- `js/stdlib/vendor`

Built-in DBSP modules include:

```js
const { createLogger } = require("log");
```

`log` wraps the vendored [pino](https://github.com/pinojs/pino) browser
build and emits structured JSON log lines on `console.log` / `console.error`.

Vendored third-party modules are available directly, for example:

```js
const minimist = require("minimist");
const pino = require("pino/browser");
```

An alias is also provided:

```js
const minimist = require("@dbsp/minimist");
```
