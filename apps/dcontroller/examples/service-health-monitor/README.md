# Service Health Monitor

This example shows a two-stage Δ-controller operator that builds a local `HealthView` from pod
readiness and patches each matching `Service` with a ready-pod count annotation.

The maintained tutorial is in [`doc/apps-dctl-tutorial-service-health.md`](/doc/apps-dctl-tutorial-service-health.md).
