# Concepts: The Extension API Server

Views are in-memory Kubernetes-like resources produced by DBSP and Δ-controller pipelines. They are not stored in Kubernetes `etcd`, but the embedded extension API server can expose them through a standard Kubernetes-compatible REST API. This lets you inspect, debug, watch, and, when needed, edit view objects with tools such as `kubectl` and `curl`.

The API server is optional. It can run in development mode over HTTP without authentication, or in production mode over HTTPS with JWT authentication and Kubernetes-style RBAC authorization.

Note that the extension API server serves view resources only, it does not proxy native Kubernetes resources such as Pods or Services. Views are ephemeral and in-memory so do not store anything critical there: if the manager restarts, views are rebuilt from source watches and controller pipelines.

## Configuration

The `dbsp` JavaScript runtime includes a standard library module named `apiserver`. It wraps the lower-level `kubernetes.runtime.config(...).generateKeys`, `generateConfig`, and `getConfig` APIs and parses command-line flags for one-off admin workflows.

The examples below use stdlib scripts. Set `DBSP_STDLIB` to the stdlib directory before running them:

```bash
export DBSP_STDLIB="$(pwd)/js/stdlib"
```

With `DBSP_STDLIB` set, `dbsp apiserver/generate_config ...` resolves `apiserver/generate_config` from the stdlib, calls its exported `main` function with `argv`, and exits. The command modules are import-safe: requiring them returns a function and does not run the command.

The helper also supports direct JavaScript use:

```js
const apiserver = require("apiserver");

apiserver.generateKeys(argv);
apiserver.generateConfig(argv);
apiserver.getConfig(argv);
```

Each function reads flags from `process.argv.slice(2)` by default. The runtime also exposes `argv` as a shorthand for the same script arguments, so `require("apiserver").generateConfig(argv)` works too. You can also call it from JavaScript with an options object:

```js
const yaml = apiserver.generateConfig({
  user: "viewer",
  namespaces: "default",
  profile: "viewer",
  keyFile: "apiserver.key",
  serverAddress: "localhost:8443",
  http: true,
});
```

## Generate Server Keys

Generate a private key and certificate for the embedded API server:

```bash
./js/bin/dbsp apiserver/generate_keys \
  --hostnames=localhost,127.0.0.1 \
  --tls-key-file=apiserver.key \
  --tls-cert-file=apiserver.crt
```

The private key signs user JWT tokens. The certificate is used by the HTTPS server and by `getConfig` to verify generated tokens.

Use hostnames that clients will use to reach the API server. For local port-forwarding, `localhost` and `127.0.0.1` are usually enough. For a production Service, include the DNS name or load-balancer hostname.

## Generate User Kubeconfigs

Generate a read-only kubeconfig for one namespace:

```bash
./js/bin/dbsp apiserver/generate_config \
  --user=viewer \
  --namespaces=default \
  --profile=viewer \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  > viewer.config
```

Generate a developer kubeconfig for multiple namespaces:

```bash
./js/bin/dbsp apiserver/generate_config \
  --user=developer \
  --namespaces=team-a,team-b,team-c \
  --profile=editor \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  > developer.config
```

Generate a cluster-wide admin kubeconfig:

```bash
./js/bin/dbsp apiserver/generate_config \
  --user=admin \
  --namespaces='*' \
  --profile=admin \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  > admin.config
```

For HTTP development mode, add `--http`:

```bash
./js/bin/dbsp apiserver/generate_config \
  --user=dev \
  --namespaces='*' \
  --profile=admin \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  --http \
  > dev.config
```

If you prefer not to use shell redirection, write directly to a file:

```bash
./js/bin/dbsp apiserver/generate_config \
  --user=viewer \
  --namespaces=default \
  --profile=viewer \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  --output-file=viewer.config
```

## Authorization Model

The API server uses JWT authentication and Kubernetes-style RBAC rules embedded in the token.

Authentication data:

- `username`: the user name shown to the API server.
- `namespaces`: the namespace allow-list. Use `*` for all namespaces. An empty list means no namespace restriction.
- `rules`: Kubernetes `PolicyRule` objects. An empty rule list means full RBAC access.

Authorization checks happen in two stages:

1. Namespace restrictions are checked first. Cross-namespace `LIST` and `WATCH` require wildcard namespace access.
2. RBAC rules are checked against the requested verb, API group, resource, and resource name.

The `apiserver` helper provides three profile shortcuts:

- `viewer`: `get`, `list`, and `watch` on all API groups and resources.
- `editor`: `get`, `list`, `watch`, `create`, `update`, `patch`, and `delete` on all API groups and resources.
- `admin`: all verbs on all API groups and resources.

## Custom RBAC Rules

Use `--rules` for inline JSON rules:

```bash
./js/bin/dbsp apiserver/generate_config \
  --user=viewer \
  --namespaces=production \
  --rules='[{"verbs":["get","list","watch"],"apiGroups":["*.view.dcontroller.io"],"resources":["*"]}]' \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  > viewer.config
```

Use `--rules-file` for larger policies:

```bash
cat > viewer-rules.json <<'EOF'
[
  {
    "verbs": ["get", "list", "watch"],
    "apiGroups": ["*.view.dcontroller.io"],
    "resources": ["*"]
  }
]
EOF

./js/bin/dbsp apiserver/generate_config \
  --user=viewer \
  --namespaces=production \
  --rules-file=viewer-rules.json \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  > viewer.config
```

Use `--resource-names` to restrict object-level verbs such as `get`, `update`, `patch`, and `delete`:

```bash
./js/bin/dbsp apiserver/generate_config \
  --user=restricted-viewer \
  --namespaces=production \
  --rules='[{"verbs":["get","list","watch"],"apiGroups":["*.view.dcontroller.io"],"resources":["*"]}]' \
  --resource-names=web-app,api-service \
  --tls-key-file=apiserver.key \
  --server-address=localhost:8443 \
  > restricted-viewer.config
```

Kubernetes RBAC semantics apply: `resourceNames` is ignored for collection verbs such as `list`, `watch`, `create`, and `deletecollection`.

## Inspect Kubeconfig Tokens

Inspect claims in a generated kubeconfig:

```bash
KUBECONFIG=./viewer.config \
./js/bin/dbsp apiserver/get_config \
  --tls-cert-file=apiserver.crt
```

Machine-readable output is available with `--json`:

```bash
./js/bin/dbsp apiserver/get_config \
  --kubeconfig=viewer.config \
  --tls-cert-file=apiserver.crt \
  --json
```

## Starting the API Server

The helper module creates keys and kubeconfigs. Your controller script still starts the embedded API server through the Kubernetes runtime configuration.

Development mode uses HTTP and no auth:

```js
kubernetes.runtime.config({
  apiServer: {
    addr: "0.0.0.0",
    port: 8443,
    http: true,
    insecure: true,
  },
}).start();
```

Production mode uses HTTPS and JWT auth:

```js
kubernetes.runtime.config({
  apiServer: {
    addr: "0.0.0.0",
    port: 8443,
    certFile: "apiserver.crt",
    keyFile: "apiserver.key",
  },
  auth: {
    privateKeyFile: "apiserver.key",
    publicKeyFile: "apiserver.crt",
  },
}).start();
```

In the Helm deployment, the repository-provided `dcontroller/dcontroller.js` script reads runtime settings from environment variables set by the chart.

## Accessing Views With Kubectl

After generating a kubeconfig and exposing the API server, point `kubectl` at the generated config:

```bash
export KUBECONFIG=./viewer.config
kubectl api-resources
```

View API groups are generated from operator names. The group format is:

```text
<operator-name>.view.dcontroller.io
```

The version is `v1alpha1`. For example:

```text
NAME         APIVERSION                                      NAMESPACED   KIND
healthview   svc-health-operator.view.dcontroller.io/v1alpha1 true         HealthView
```

Get a specific view object:

```bash
kubectl get healthview.svc-health-operator.view.dcontroller.io web-app -n default -o yaml
```

List all view objects in a namespace:

```bash
kubectl get healthview.svc-health-operator.view.dcontroller.io -n default
```

Watch a view for changes:

```bash
kubectl get healthview.svc-health-operator.view.dcontroller.io -n default --watch
```

Cluster-wide list and watch operations require `--namespaces='*'` in the token:

```bash
kubectl get healthview.svc-health-operator.view.dcontroller.io --all-namespaces
```

## Troubleshooting

- Set `DBSP_STDLIB` to the stdlib directory before running `dbsp apiserver/...` commands.
- If you use the fallback `-e` form, add `--` after the JavaScript expression; otherwise flags such as `--user` are parsed as `dbsp` flags.
- Add `--http` when generating a kubeconfig for a development-mode HTTP API server.
- Add `--insecure` when generating a kubeconfig for HTTPS with a self-signed certificate that clients should not verify.
- Make sure `--server-address` is the address clients use, not necessarily the bind address inside the manager.
- If `kubectl` cannot list across all namespaces, regenerate the kubeconfig with `--namespaces='*'`.
- If token inspection fails, check that `--tls-cert-file` points to the certificate matching the private key used to sign the token.
