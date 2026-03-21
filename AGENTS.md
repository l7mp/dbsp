# AGENTS.md

This file guides agentic coding agents working in this repository.

## Project Snapshot

- Project: DBSP, an incremental computation library in Go.
- Module path: `github.com/l7mp/dbsp`.
- Go version: `go 1.24.0` with toolchain `go1.24.9`.
- Test stack: Ginkgo v2 + Gomega (`go test` runner).

## Build, Lint, and Test Commands

Prefer `go test` and package-scoped commands. Keep commands deterministic.

### Build

```bash
# Build all packages (compilation check)
go test ./... -run '^$'

# Build CLI binary (from Makefile)
make build
```

### Lint / Static Analysis

```bash
# Format + organize imports (required after Go edits)
goimports -w .

# Basic static checks
go vet ./...
```

Notes:
- There is no repository-specific lint config file.
- If `staticcheck` is installed locally, it can be used as an optional extra check.

### Test (all)

```bash
go test ./...
```

### Test (all, no cache, verbose)

```bash
go test ./... -v -count=1
# equivalent helper target
make test
```

### Test (single package)

Use real package paths from this repository:

```bash
go test ./dbsp/zset
go test ./dbsp/operator
go test ./dbsp/circuit
go test ./dbsp/transform
go test ./dbsp/executor
go test ./compiler/sql
go test ./engine
go test ./cmd/dbsp
```

### Test (single test / focused run)

For Ginkgo suites, run a specific package and filter with regex:

```bash
# Match suite/test names via -run regex
go test ./dbsp/operator -run 'Select'

# Explicitly target ginkgo test descriptions
go test ./dbsp/operator -ginkgo.focus='negates all weights'

# Combine both when needed
go test ./dbsp/operator -run 'Operators' -ginkgo.focus='LinearCombination'
```

Tips:
- `-run` filters Go test names and works with Ginkgo-generated test entry points.
- `-ginkgo.focus` filters `Describe` / `Context` / `It` text.
- Use package-level execution first, then tighten filters.

## Repository-Specific Rules

- Cursor rules check: no `.cursor/rules/` and no `.cursorrules` found.
- Copilot rules check: no `.github/copilot-instructions.md` found.

## Architecture Quick Map

- `dbsp/zset`: Weighted multisets (Z-sets), foundational data structure.
- `dbsp/operator`: Operators classified by linearity.
- `dbsp/circuit`: Circuit graph model (inputs, outputs, ops, delay/integrate/differentiate nodes).
- `dbsp/transform`: Incrementalization logic (Algorithm 6.4 style transform).
- `dbsp/executor`: Runtime execution engine for circuits.
- `compiler/sql`: SQL planner/compiler layers.
- `engine`: Top-level engine orchestration.
- `expression` and `expression/dbsp`: Expression model and DBSP integration.
- `datamodel/*`: Relational/unstructured data helpers.

## Code Style Guidelines

### Imports and Formatting

- Always run `goimports -w .` after edits; do not hand-sort imports.
- Keep imports minimal; remove unused imports promptly.
- Follow standard Go formatting (`gofmt`-compatible layout).
- Avoid manual column alignment or cosmetic whitespace churn.

### Naming Conventions

- Exported identifiers use `CamelCase`; unexported use `camelCase`.
- Prefer short local names and explicit public API names.
- Use domain terms consistently: `zset`, `delta`, `state`, `circuit`, `operator`.
- Keep acronyms Go-idiomatic (`ID`, `SQL`, etc.).

### Types and Interfaces

- Prefer concrete return types unless abstraction is required.
- Accept interfaces when consuming behavior; return concrete types when practical.
- Keep interfaces small and focused.
- Use `any` only where polymorphism is intentional and documented.
- Preserve algebraic behavior expectations in Z-set and operator code.

### Error Handling

- Return errors for expected failures; do not panic in normal control flow.
- Add context when propagating errors: `fmt.Errorf("compile query: %w", err)`.
- Prefer `errors.Is` / `errors.As` for matching.
- Handle errors immediately; avoid deep nesting and `err` shadowing.

### Comments and Documentation

- Write complete sentences ending with punctuation.
- Comment non-obvious invariants, math, and edge-case logic.
- Avoid comments that restate obvious code.
- Mathematical symbols used in this codebase are acceptable (`σ`, `×`, `∫`, `D`, `z⁻¹`, `δ₀`).

### Testing Style

- **ALWAYS** write tests in Ginkgo v2 + Gomega style.
- Avoid plain `testing`-only test cases for behavioral coverage.
- Add or reuse a `*_suite_test.go` entrypoint in each package with tests.
- Prefer focused, composable fixtures over large shared mutable setup.
- Validate both normal and incremental behavior where relevant.
- Keep test names descriptive and behavior-oriented.

## Domain Conventions (DBSP)

- Linear operators are self-incremental (`O^Δ = O`).
- Bilinear operators follow three-term delta expansion.
- Non-linear operators are handled through integrate/differentiate transforms.
- Every circuit cycle must include a delay node; use validation utilities.

## Workflow Expectations for Agents

- Read `README.md`, `CLAUDE.md`, and this file before large changes.
- Keep changes scoped; avoid unrelated refactors.
- Do not add dependencies unless required; update `go.mod` and `go.sum` together.
- Avoid touching generated artifacts or fixtures unless task-specific.
- Prefer package-local verification for faster iteration, then run broader tests.
- Do not use marshal/unmarshal conversion as a generic object adaptation trick (for example, converting between unstructured/document forms by serializing to JSON and parsing back) without explicit user approval first. If you think it is necessary, stop and ask.

## Commit Message Convention

- **ALWAYS** use the commit format `type(component): Message`.
- Prefer component names like `conn/misc`, `runtime`, `conn/k8s`, `compiler`, etc.
- Start the subject with a capitalized imperative phrase, only capitalize the first word.
- Example: `feat(conn/misc): Add Pipe producer and consumer`.

## Useful Command Reference

```bash
# Core loop
go test ./dbsp/operator -run 'Select'
go test ./dbsp/operator -ginkgo.focus='LinearCombination'
go test ./... -v -count=1
go vet ./...
goimports -w .

# Makefile helpers
make build
make test
make clean
```
