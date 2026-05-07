# Δ-controller: Getting Started

Δ-controller lives in the [`dcontroller/`](dcontroller/) module and builds on the same Go workspace
as DBSP. This guide is the primary place for install and API-server configuration.

## Installation

### Obtain `dctl`

`dctl` is used to run Δ-controller locally and to generate API-server certificates and kubeconfigs.

- Local build: `make build` then use `./dcontroller/bin/dctl`.
- Go install: `go install github.com/l7mp/dbsp/dcontroller@latest`.
- Release artifact: not published yet.

### Build binaries from source

From the workspace root:

```bash
make build
```

This builds `dctl` and the example binaries under `dcontroller/examples/`.

To build only the Δ-controller module:

```bash
cd dcontroller
make build
```

### Install with Helm (default development mode)

Add the Helm repo:

```bash
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
```

Install into `dcontroller-system`:

```bash
helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system \
  --create-namespace
```

Default chart mode is `development`:

- HTTP API server.
- Authentication disabled.
- Internal `ClusterIP` service.

If you are editing the chart from this repo, use `./dcontroller/chart/helm` instead of
`dcontroller/dcontroller`.

## Configuration and API access

### Development mode access

Port-forward the API server:

```bash
kubectl -n dcontroller-system port-forward svc/dcontroller-apiserver 8443:8443
```

Generate and use a development kubeconfig:

```bash
./dcontroller/bin/dctl generate-keys --hostname=localhost
./dcontroller/bin/dctl generate-config --http --user=dev --namespaces="*" \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > /tmp/dctl.config
export KUBECONFIG=/tmp/dctl.config
kubectl api-resources
```

### Production mode (TLS + authentication)

Production mode needs a TLS Secret. Without it, the manager container does not start.

```bash
./dcontroller/bin/dctl generate-keys \
  --hostname=localhost \
  --hostname=dcontroller-apiserver.dcontroller-system.svc

kubectl -n dcontroller-system create secret tls dcontroller-tls \
  --cert=apiserver.crt \
  --key=apiserver.key \
  --dry-run=client -o yaml | kubectl apply -f -

helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system \
  --set apiServer.mode=production
```

### Expose production API server

Keep `ClusterIP` and use port-forward, or expose externally:

```bash
# LoadBalancer
helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system \
  --set apiServer.mode=production \
  --set apiServer.service.type=LoadBalancer

# NodePort
helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system \
  --set apiServer.mode=production \
  --set apiServer.service.type=NodePort \
  --set apiServer.service.nodePort=30443

# Gateway API
helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system \
  --set apiServer.mode=production \
  --set apiServer.gateway.enabled=true \
  --set apiServer.gateway.gatewayName=my-gateway \
  --set apiServer.gateway.gatewayNamespace=gateway-system \
  --set apiServer.gateway.hostname=dcontroller-api.example.com
```

If exposed externally, include the external hostname or IP in `dctl generate-keys --hostname=...`.

### Authentication and authorization profiles

`dctl generate-config` creates kubeconfigs with embedded JWT tokens. Access is controlled by
namespace restrictions and Kubernetes-style RBAC policy rules.

```bash
# Admin profile
./dcontroller/bin/dctl generate-config --user=admin --namespaces="*" \
  --tls-key-file=apiserver.key --insecure --server-address=localhost:8443 > /tmp/dctl-admin.config

# Read-only viewer profile
./dcontroller/bin/dctl generate-config --user=viewer --namespaces=default \
  --rules='[{"verbs":["get","list","watch"],"apiGroups":["*.view.dcontroller.io"],"resources":["*"]}]' \
  --tls-key-file=apiserver.key --insecure --server-address=localhost:8443 > /tmp/dctl-viewer.config
```

For long-lived tokens, set `--expiry` (for example `--expiry=720h`).

### Helm values quick reference

| Value | Meaning | Default |
|---|---|---|
| `apiServer.enabled` | Enable embedded API server | `true` |
| `apiServer.mode` | `development` or `production` | `development` |
| `apiServer.port` | API server port | `8443` |
| `apiServer.tls.secretName` | TLS secret name | `dcontroller-tls` |
| `apiServer.service.type` | `ClusterIP`, `LoadBalancer`, `NodePort` | `ClusterIP` |
| `apiServer.service.nodePort` | NodePort number | unset |
| `apiServer.service.annotations` | Service annotations | `{}` |
| `apiServer.gateway.enabled` | Create `HTTPRoute` | `false` |

For all chart values, see `dcontroller/chart/helm/values.yaml`.

### Upgrade, uninstall, troubleshooting

```bash
# Upgrade
helm repo update
helm upgrade dcontroller dcontroller/dcontroller --namespace dcontroller-system

# Uninstall
helm uninstall dcontroller --namespace dcontroller-system
kubectl delete namespace dcontroller-system

# Logs
kubectl logs -n dcontroller-system deployment/dcontroller-manager
```

## Run manager locally (without Helm)

`dctl` can run the manager directly against your current Kubernetes context.

```bash
# Development mode
./dcontroller/bin/dctl start --http --disable-authentication

# TLS-enabled mode
./dcontroller/bin/dctl generate-keys --hostname=localhost
./dcontroller/bin/dctl start --insecure --tls-cert-file=apiserver.crt --tls-key-file=apiserver.key
```

## Apply an operator

Apply the service-health example:

```bash
kubectl apply -f dcontroller/examples/service-health-monitor/svc-health-operator.yaml
kubectl apply -f dcontroller/examples/service-health-monitor/web-app.yaml
kubectl get operator svc-health-operator -o yaml
```

Useful status fields: `status.conditions` and `status.lastErrors`.

## Inspect views

```bash
kubectl api-resources
kubectl get healthview.svc-health-operator.view.dcontroller.io -o yaml
kubectl get healthview.svc-health-operator.view.dcontroller.io --watch
```

Views appear under API group `<operator-name>.view.dcontroller.io/v1alpha1`.
