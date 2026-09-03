# Δ-controller Helm chart

Deploys Δ-controller: the `dbsp` JavaScript runtime running the dcontroller
manager, the `Operator` CRD, and the RBAC the hosted operators need.

```console
helm repo add dcontroller https://l7mp.github.io/dbsp/dcontroller/
helm repo update
helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system --create-namespace
```

Documentation lives in `/doc/`:

- [Getting started](/doc/apps-dctl-getting-started.md)
- [Extension API Server workflows](/doc/connectors-kubernetes-API-server.md)
