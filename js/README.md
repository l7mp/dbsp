# JavaScript runtime for DBSP

`dbsp` runs JavaScript DBSP scripts through an embedded JS runtime.

Available globals:

- `sql`, `aggregate`
- `producer(topic[, entries])`, `consumer(topic, fn)`
- `publish(topic, entries)`, `subscribe(topic, fn)` as shorthand helpers
- `runtime.*` aliases for all APIs (`runtime.sql`, `runtime.aggregate`, `runtime.producer`, `runtime.consumer`, `runtime.publish`, `runtime.subscribe`)
- `runtime.onError(fn)` for asynchronous runtime errors (`{ origin, message }`)

## Getting started

Build the executable.

```bash
go build -o dbsp .
```

Run a script.

```bash
./dbsp examples/join_project.js
```

Use `-l, --loglevel debug|info|warn|error` (or `-v`) to configure runtime logs.
