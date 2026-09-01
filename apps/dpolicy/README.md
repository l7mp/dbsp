# Δ-policy

A continuous audit controller for [Gatekeeper](https://open-policy-agent.github.io/gatekeeper/)
policies, built as a DBSP incremental circuit. The full guide is
[doc/apps-dpol-overview.md](../../doc/apps-dpol-overview.md). It consumes stock Gatekeeper
objects - ConstraintTemplates and their constraints - evaluates the Rego policies against the
watched resources, and maintains each constraint's audit status (`totalViolations` plus the capped
`violations` list) as a continuously reconciled output. Where Gatekeeper's audit controller
re-evaluates every constraint against every resource on a fixed interval, Δ-policy evaluates
deltas: a violation appears in the status when the violating change happens and disappears when it
is reverted, at O(|change|) cost and with no audit interval in between.

Policy evaluation runs the real Rego expression operator, so existing templates work
verbatim. Constraints using match features the auditor does not implement (`namespaceSelector`,
`scope`, `name`, ...) are rejected.

## Quickstart

```bash
# Build the runtime.
cd js && go build -o bin/dbsp ./cmd && cd ..

# Self-contained test suite (no cluster needed).
./js/bin/dbsp apps/dpolicy/index.js test

# Against a cluster with the Gatekeeper CRDs (constraint kinds are static
# config; every kind is one Gatekeeper-generated CRD).
./js/bin/dbsp apps/dpolicy/index.js controller --constraint-kinds K8sRequiredLabels
```

The controller reconciles the status of the audited constraints and streams the audit log - every
violation raised or cleared, every rejected policy - as JSONL on stdout.

Violations exist in three forms, one per consumer: the **constraint status** is the
Gatekeeper-compatible summary (`totalViolations` plus the capped list), the **Violation view** is
the complete current set served as objects by the embedded API extension server (`kubectl get
violations`, and the **log stream** is the event history.

## License

Copyright 2026 by its authors. See [AUTHORS](/AUTHORS).

MIT License. See [LICENSE](/LICENSE).
