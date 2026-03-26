# Δ-controller Getting Started

Δ-controller lives in the `dcontroller/` module and builds on the same Go workspace as the rest of
the project. If you want to explore the code or run the examples locally, the easiest path is to
build from a checkout of this repository.

## Build the binaries

From the workspace root:

```bash
make build
```

This includes the `dctl` binary and the example binaries under `dcontroller/examples/`.
If you only need the controller manager and CLI, build that module directly:

```bash
cd dcontroller
make build
```

The main binary is `dcontroller/bin/dctl`.

## Run the manager locally

The `dctl` binary starts the controller manager and also provides helper commands for the embedded
API server.

For local development against your current Kubernetes context, the simplest start mode is:

```bash
./dcontroller/bin/dctl start --http --disable-authentication
```

This starts the operator manager and, unless disabled, the embedded API server that exposes local
views. For a more realistic setup, generate a certificate pair and run with TLS enabled:

```bash
./dcontroller/bin/dctl generate-keys --hostname=localhost
./dcontroller/bin/dctl start --insecure --tls-cert-file=apiserver.crt --tls-key-file=apiserver.key
```

## Apply an operator

Once the manager is running, apply one of the bundled examples. For instance, the service health
example is a compact demonstration of views and multi-stage pipelines:

```bash
kubectl apply -f dcontroller/examples/service-health-monitor/svc-health-operator.yaml
kubectl apply -f dcontroller/examples/service-health-monitor/web-app.yaml
```

Check that the operator is ready:

```bash
kubectl get operator svc-health-operator -o yaml
```

In the current implementation, the important top-level status fields are `status.conditions` and
`status.lastErrors`.

## Inspect local views

If the embedded API server is enabled, you can inspect Δ-controller views with a separate kubeconfig.
For local HTTP development mode:

```bash
./dcontroller/bin/dctl generate-config --http --user=dev --namespaces="*" \
  --server-address=localhost:8443 > /tmp/dctl.config
KUBECONFIG=/tmp/dctl.config kubectl api-resources
```

Once an operator creates a view, it will appear under the API group
`<operator-name>.view.dcontroller.io/v1alpha1`.
