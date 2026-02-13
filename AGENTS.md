# AGENTS.md

This file guides agentic coding agents working in this repository.

## Project Snapshot

- Name: DBSP (incremental computation library in Go).
- Module: `github.com/l7mp/dbsp`.
- Go: `go 1.24.0` with toolchain `go1.24.9`.
- Testing: Ginkgo v2 + Gomega (BDD style).

## Build, Test, Lint

Use `go test` as the primary build/test command. There is no repo-provided lint config.

### Build

```bash
go test ./...
```

### Test (all)

```bash
go test ./...
```

### Test (single package)

```bash
go test ./zset
go test ./operator
go test ./circuit
go test ./transform
go test ./execute
go test ./sql
go test ./relational
```

### Test (single test)

Ginkgo v2 tests use `go test` with `-run` and a regex that matches `Describe`/`It` names.

```bash
go test ./operator -run "Select"
```

### Test (verbose)

```bash
go test -v ./...
```

### Formatting

Use `goimports` to format and organize imports.

```bash
goimports -w .
```

## Repository-Specific Rules

- There are no Cursor rules in `.cursor/rules/` or `.cursorrules`.
- There are no Copilot rules in `.github/copilot-instructions.md`.

## Architecture Quick Map

- `zset/`: Z-sets (weighted multisets), core data structure. Elements implement `Key()`.
- `expr/`: Expressions for predicates and projections. `Evaluate(elem) (any, error)`.
- `operator/`: Linear, bilinear, non-linear operators.
- `circuit/`: Directed graph model (Input, Output, Operator, Delay, Integrate, Differentiate, Delta0).
- `transform/`: Algorithm 6.4 incrementalization.
- `execute/`: Runtime executor.
- `relational/`: SQL-like row helpers.
- `sql/`: SQL compiler/executor.

## Code Style Guidelines

### Formatting and Imports

- Use `goimports` for formatting and import grouping.
- Keep imports minimal and sorted by `goimports`.
- Avoid manual alignment in Go (no extra spacing for columns).
- Keep line length readable; prefer wrapping logical clauses.

### Comments

- Follow Go style guide: full sentences ending with periods.
- Mathematical notation in comments is accepted: `σ`, `×`, `∫`, `D`, `z⁻¹`, `δ₀`.
- Use comments to explain non-obvious logic or math, not to restate code.

### Naming

- Use Go idioms: `CamelCase` for exported, `camelCase` for unexported.
- Prefer short, clear names in local scopes; be explicit in public APIs.
- Domain names used in this repo: `zset`, `circuit`, `operator`, `transform`, `execute`.
- Prefer `delta` or `Δ` in comments for changes, and `state` or `s` for full streams.

### Types and Interfaces

- Elements implement `Key()` for identity in Z-sets.
- Prefer small interfaces; accept interfaces, return concrete types when possible.
- Use `any` only when the API is intentionally generic; document expectations.
- Keep Z-set operations consistent with algebraic properties (linearity, bilinearity).

### Error Handling

- Return errors explicitly; do not panic for expected failures.
- Wrap errors with context using `fmt.Errorf("...: %w", err)`.
- Avoid sentinel errors unless already established; prefer `errors.Is`/`errors.As`.
- Check errors immediately; avoid shadowing `err` in long blocks.

### Testing

- Use Ginkgo v2 + Gomega style:

```go
var _ = Describe("Feature", func() {
    It("should do something", func() {
        Expect(result).To(Equal(expected))
    })
})
```

- Key pattern: verify normal execution equals incremental execution.
- Prefer small, composable fixtures over large shared state.

### Circuit and Operator Conventions

- Linear operators are their own incremental version: `O^Δ = O`.
- Bilinear operators use the three-term expansion with integrators.
- Non-linear operators wrap with `∫` then `D`.
- Every circuit cycle must include a delay (`z⁻¹`); use `circuit.Validate()`.

### SQL Package Notes

- SQL layer builds circuits via compilation and execution; keep it deterministic.
- Prefer explicit encoding/decoding boundaries in `sql/encoding.go`.

## Workflow Expectations for Agents

- Read `CLAUDE.md` and `README.md` for project context.
- Use `goimports -w .` after Go edits.
- Avoid adding new dependencies unless required; update `go.mod`/`go.sum` together.
- Do not modify generated or test fixtures unless the task requires it.
- Keep changes scoped to the request; do not refactor unrelated code.

## Useful Reference Commands

```bash
go test ./...
go test ./operator -run "Select"
go test -v ./...
goimports -w .
```
