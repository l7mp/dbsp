# Δ-controller: Getting Started

Δ-controller now runs as a JavaScript program inside the `dbsp` runtime. The
old `dctl` admin workflow has been replaced by the `kubernetes.runtime` JS API.

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

## Pure admin utilities (no runtime start)

`kubernetes.runtime.config(...)` also exposes stateless utility functions.
These do not start any runtime component.

```js
const runtimeConfig = kubernetes.runtime.config({
  apiServer: { addr: "localhost", port: 8443, http: true },
});

runtimeConfig.generateKeys({
  hostnames: ["localhost"],
  keyFile: "apiserver.key",
  certFile: "apiserver.crt",
});

const kubeconfigYAML = runtimeConfig.generateKubeConfig({
  user: "dev",
  namespaces: ["*"],
  keyFile: "apiserver.key",
  serverAddress: "localhost:8443",
  http: true,
});

const info = runtimeConfig.inspectKubeConfig({
  kubeconfig: "/tmp/dcontroller.config",
  certFile: "apiserver.crt",
});
```

For one-off admin tasks, prefer inline execution with `dbsp -e`.

```bash
./js/bin/dbsp -e 'const cfg = kubernetes.runtime.config({apiServer:{addr:"localhost",port:8443,http:true}}); const yaml = cfg.generateKubeConfig({user:"dev",namespaces:["*"],keyFile:"apiserver.key",serverAddress:"localhost:8443",http:true}); require("fs").writeFileSync("/tmp/dcontroller.config", yaml);'
```

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

One practical workflow is to use an inline one-liner:

```bash
./js/bin/dbsp -e 'const cfg = kubernetes.runtime.config({apiServer:{addr:"localhost",port:8443,http:true}}); const yaml = cfg.generateKubeConfig({user:"dev",namespaces:["*"],keyFile:"apiserver.key",serverAddress:"localhost:8443",http:true}); require("fs").writeFileSync("/tmp/dcontroller.config", yaml);'
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
