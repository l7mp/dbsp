# Δ-controller: Getting Started

Δ-controller runs as a JavaScript program inside the `dbsp` runtime. API-server
admin workflows use the `apiserver` JS standard library module; see
[Concepts: The Extension API Server](/doc/concepts-API-server.md) for the full
guide.

## Build and install

Build the JS runtime binary from the workspace root:

```bash
make -C js build
```

This produces `js/bin/dbsp`.

Build the dcontroller image and chart assets from `dcontroller/` as needed:

```bash
make -C dcontroller docker-build
```

## Local runtime model

Kubernetes connector startup is explicit. Scripts must call
`kubernetes.runtime.start()` before `kubernetes.watch/list/patch/update/log`.

```js
// minimal startup: native Kubernetes only, no embedded API server
kubernetes.runtime.start();
```

To include embedded API-server settings and auth defaults, create a runtime
config object first:

```js
const runtimeConfig = kubernetes.runtime.config({
  apiServer: {
    addr: "0.0.0.0",
    port: 8443,
    http: false,
    insecure: true,
    certFile: "apiserver.crt",
    keyFile: "apiserver.key",
  },
  auth: {
    privateKeyFile: "apiserver.key",
    publicKeyFile: "apiserver.crt",
  },
});

runtimeConfig.start();
```

## API server admin utilities

Use the `apiserver` standard library module for key generation, user
kubeconfig generation, and token inspection. These utilities do not start any
runtime component. Set `DBSP_STDLIB` before running the stdlib script commands:

```bash
export DBSP_STDLIB="$(pwd)/js/stdlib"

./js/bin/dbsp apiserver/generate_keys \
  --hostnames=localhost,127.0.0.1 \
  --tls-key-file=apiserver.key \
  --tls-cert-file=apiserver.crt

./js/bin/dbsp apiserver/generate_config \
  --user=dev \
  --namespaces='*' \
  --profile=admin \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  --http \
  > /tmp/dcontroller.config
```

See [Concepts: The Extension API Server](/doc/concepts-API-server.md) for
RBAC profiles, custom rules, and production HTTPS examples.

## Helm install

```bash
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system \
  --create-namespace
```

The deployment runs `/dbsp <script>`. API-server behavior is expected to be
declared in the script via `kubernetes.runtime.config(...).start()`.
The repository-provided manager script is `dcontroller/dcontroller.js`.

## Script-driven configuration pattern

Mount a config file and let the script own runtime policy:

```js
const fs = require("fs");

const raw = fs.readFileSync("/etc/dcontroller/runtime.json", "utf8");
const cfg = JSON.parse(raw);
const runtimeConfig = kubernetes.runtime.config(cfg.kubernetes || {});

runtimeConfig.start();
```

This keeps Helm focused on deployment concerns while connector behavior stays
in source-controlled JS code.

## Generate kubeconfig for view inspection

One practical workflow is to generate a kubeconfig for the embedded API server
and then point `kubectl` at it:

```bash
export DBSP_STDLIB="$(pwd)/js/stdlib"

./js/bin/dbsp apiserver/generate_config \
  --user=dev \
  --namespaces='*' \
  --profile=admin \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  --http \
  > /tmp/dcontroller.config
KUBECONFIG=/tmp/dcontroller.config kubectl api-resources
```

## Troubleshooting

- If you see `kubernetes runtime is not started`, call
  `kubernetes.runtime.start()` early in your script.
- If kubeconfig is unavailable, native resources are disabled but view
  resources can still work when the embedded API server is configured.
- For runtime failures from async components, register `runtime.onError(...)`.

## Apply an example operator

```bash
kubectl apply -f dcontroller/examples/service-health-monitor/svc-health-operator.yaml
kubectl apply -f dcontroller/examples/service-health-monitor/web-app.yaml
kubectl get operator svc-health-operator -o yaml
```

Useful status fields: `status.conditions` and `status.lastErrors`.
