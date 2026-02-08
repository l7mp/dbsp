# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DBSP is a Go implementation of the DBSP circuit library for incremental computation. It translates DBSP theory (Z-sets, streams, operators, incrementalization) into a practical library.

## Build and Test Commands

```bash
# Run all tests
go test ./...

# Run tests for a specific package
go test ./zset
go test ./operator
go test ./circuit
go test ./transform
go test ./execute

# Run a specific test (Ginkgo uses -run with regex on Describe/It names)
go test ./operator -run "Select"

# Run tests with verbose output
go test -v ./...

# Format and organize imports
goimports -w .
```

## Architecture

### Package Structure

- **zset/**: Z-sets (weighted multisets) - the fundamental data structure. Elements implement `Key()` for identity.
- **expr/**: Expression interface with single `Evaluate(elem) (any, error)` method for predicates and projections.
- **operator/**: All DBSP operators classified by linearity:
  - Linear (`O^Δ = O`): Negate, Plus, Select, Project
  - Bilinear (three-term expansion): CartesianProduct
  - Non-linear (`D ∘ O ∘ ∫` wrapping): Distinct, Group, Unwind
- **circuit/**: Circuit model as directed graph. Node types: Input, Output, Operator, Delay (z⁻¹), Integrate (∫), Differentiate (D), Delta0 (δ₀).
- **transform/**: Implements Algorithm 6.4 - transforms circuit C into incremental version C^Δ.
- **execute/**: Runtime executor with state management for delays and integrators.

### Key Design Principles

1. **Circuits are first-class**: Everything is a circuit, including operators.
2. **No per-operator incrementalization**: Use Algorithm 6.4 (circuit transformation) instead.
3. **Mutable Z-sets**: `Insert` modifies in-place.
4. **Interface-based elements**: Users define their own element types implementing `Element.Key()`.

### Well-Formedness

Every cycle in a circuit must contain at least one delay (z⁻¹) node. Use `circuit.Validate()` to check.

## Testing

Tests use Ginkgo v2 + Gomega (BDD style):

```go
var _ = Describe("Feature", func() {
    It("should do something", func() {
        Expect(result).To(Equal(expected))
    })
})
```

Key test pattern: verify normal execution equals incremental execution.

## Code Style

- Comments follow Go style guide - complete sentences ending with periods.
- Mathematical notation in comments: σ (select), × (product), ∫ (integrate), D (differentiate), z⁻¹ (delay), δ₀ (delta-zero).
