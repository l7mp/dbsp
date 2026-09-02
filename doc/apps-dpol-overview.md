# Δ-policy: a continuous Gatekeeper-audit controller

Δ-policy is a continuous audit controller for
[OPA Gatekeeper](https://open-policy-agent.github.io/gatekeeper/) constraints: it evaluates
Gatekeeper `ConstraintTemplates` and constraints against the watched cluster state continuously
and incrementally, instead of on Gatekeeper's periodic audit timer. Where a stock audit pass
reports violations minutes after the fact, Δ-policy's violation set is a live incremental view:
a violating object is reported the moment its watch event arrives, and the report retracts the
moment the violation clears. The code lives in `apps/dpolicy/`.


## What it does

- Watches `ConstraintTemplates` and the constraint kinds they define, plus the object kinds the
  constraints match.
- Evaluates the templates' Rego through the `@rego` expression operator (the Kubernetes
  connector's contributed operator, running OPA in-process): each constraint becomes an
  incremental pipeline from matched objects to violations.
- Writes the violation totals and details back to the constraints' `status`, the same surface
  Gatekeeper's own audit fills, through Patchers.
- Optionally materializes per-constraint `Violation` views, inspectable with kubectl through the
  embedded API server.

## Modes

Like Δ-gateway, the control loop is selectable with `--mode`: `reconciler` (the closed loop,
default), `open`, and `smith` (dead-time compensated), and `sotw` (the snapshot-adapted
execution, the same programs with empty transform chains). The pipelines are identical in every
mode.

## Running

```bash
./js/bin/dbsp apps/dpolicy/index.js test          # self-contained suite, no cluster
./js/bin/dbsp apps/dpolicy/index.js controller    # against a live cluster
```

## Why it exists

Gatekeeper's audit is a snapshot recomputation on a timer: the whole cluster is re-evaluated every
interval, and anything that changes faster than the interval is invisible (a Nyquist problem, and
a measurable one). Δ-policy is the incremental restatement of the same computation: the audit
becomes a standing dataflow, violations are maintained rather than recomputed, and detection
latency is bounded by watch latency instead of the audit period.
