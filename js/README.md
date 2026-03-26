# JavaScript runtime for DBSP

`dbsp` runs JavaScript DBSP scripts through an embedded JS runtime.

Available globals:

- `sql`, `aggregate`
- `producer(topic[, entries])`, `consumer(topic, fn)`
- `publish(topic, entries)`, `subscribe(topic, fn)` as shorthand helpers
- `runtime.*` aliases for all APIs (`runtime.sql`, `runtime.aggregate`, `runtime.producer`, `runtime.consumer`, `runtime.publish`, `runtime.subscribe`)
- `runtime.onError(fn)` for asynchronous runtime errors (`{ origin, message }`)
- `runtime.observe(circuitName, fn)` to observe execution of a validated circuit by name
- `circuit.observe(fn)` on compiled handles (before/after `validate()`)
- `cancel()` to stop the current execution context (inside callbacks: stop that callback owner; at top level: stop the script)

## Getting started

Build the executable.

```bash
go build -o dbsp .
```

Run a script.

```bash
./dbsp examples/join_project.js
./dbsp examples/observer-demo.js
```

Use `-l, --loglevel debug|info|warn|error` (or `-v`) to configure runtime logs.

## Cancellation semantics

- `cancel()` is context-sensitive and always targets the current execution context.
- In a callback (`consumer`, `subscribe`, `circuit.observe`, `runtime.observe`), `cancel()` stops that callback owner.
- At top level (outside callbacks), `cancel()` stops the script VM.

Example:

```js
consumer("obs-output", (entries) => {
  console.log(entries);
  cancel();
});
```
