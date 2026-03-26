# Extension API Server

The embedded API server exposes those in-memory views so that normal tools such as `kubectl` can
list, get, watch, create, update, and delete them. The API server only serves views. Native
Kubernetes resources such as `Pod`, `Service`, or `Deployment` still belong to the main cluster API
server.

## Development access

If Δ-controller is running locally in HTTP mode with authentication disabled, generate a matching
kubeconfig:

```bash
dctl generate-config --http --user=dev --namespaces="*" \
  --server-address=localhost:8443 > /tmp/dctl.config
```

Then point `kubectl` at it:

```bash
KUBECONFIG=/tmp/dctl.config kubectl api-resources
```

Once an operator creates a view, you will see resource names under the API group
`<operator-name>.view.dcontroller.io/v1alpha1`.

## TLS and authorization

The current implementation also supports TLS and JWT-based authorization.

Generate a key pair:

```bash
dctl generate-keys --hostname=localhost
```

Start the manager with TLS:

```bash
dctl start --insecure --tls-cert-file=apiserver.crt --tls-key-file=apiserver.key
```

Create a kubeconfig with restricted read-only access:

```bash
dctl generate-config --user=viewer --namespaces=default \
  --rules='[{"verbs":["get","list","watch"],"apiGroups":["*.view.dcontroller.io"],"resources":["*"]}]' \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > viewer.config
```

The authorization model combines namespace restrictions with Kubernetes-style policy rules. For
most users there are only two common cases: a restrictive viewer profile with read-only access to
selected namespaces, and an open development profile with wildcard namespace access and no custom
rule restriction.

Restrictive profile:

```bash
dctl generate-config --user=viewer --namespaces=default \
  --rules='[{"verbs":["get","list","watch"],"apiGroups":["*.view.dcontroller.io"],"resources":["*"]}]' \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > viewer.config
```

Open profile:

```bash
dctl generate-config --user=dev --namespaces="*" \
  --tls-key-file=apiserver.key --server-address=localhost:8443 > dev.config
```

## Inspecting views

Use the fully qualified resource name when talking to the embedded API server. For example, after
deploying the service health example:

```bash
KUBECONFIG=./dev.config kubectl get healthview.svc-health-operator.view.dcontroller.io -o yaml
```

To watch a view in real time:

```bash
KUBECONFIG=./dev.config kubectl get healthview.svc-health-operator.view.dcontroller.io --watch
```

This API is mostly a debugging and observability tool. Since views are in-memory, they disappear on
manager restart and are rebuilt from the sources as reconciliation resumes.
