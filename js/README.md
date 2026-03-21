# JavaScript runtime for DBSP

`dbsp` runs JavaScript DBSP scripts through an embedded JS runtime.

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

