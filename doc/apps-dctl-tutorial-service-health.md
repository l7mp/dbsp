# Tutorial: Service Health Monitor

This example shows how to chain two circuits through an internal stream. The result is a small
operator that annotates each `Service` with a ready-pod count such as `2/2`. The first circuit
watches labeled pods and reduces them onto the internal `HealthView` stream. The second circuit
joins that stream with native `Service` objects and patches the result back onto the service.

The example files live in `apps/dcontroller/examples/service-health-monitor/`.

## Apply the operator and workload

```bash
kubectl apply -f apps/dcontroller/examples/service-health-monitor/svc-health-operator.yaml
kubectl apply -f apps/dcontroller/examples/service-health-monitor/web-app.yaml
```

## Verify the annotation

The service should receive an annotation like `dcontroller.io/pod-ready: 2/2`.

```bash
kubectl get service web-app -o jsonpath='{.metadata.annotations.dcontroller\.io/pod-ready}'
```

You can also inspect the operator status:

```bash
kubectl get operator svc-health-operator -o yaml
```

Look for `status.conditions` and `status.lastErrors`.

## Inspect the intermediate view

If the embedded API server is enabled, the example is a good place to inspect a local view.

Generate a kubeconfig for the view API server via the `apiserver` standard
library module:

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

See [Kubernetes connector: The Extension API Server](/doc/connectors-kubernetes-API-server.md) for the
full access workflow.

When the intermediate state is materialized as a view (a `HealthView` target on the first circuit
plus a `HealthView` source on the second, instead of the internal stream), the generated objects
are readable the same way:

```bash
KUBECONFIG=/tmp/dcontroller.config kubectl get healthview.svc-health-operator.view.dcontroller.io -o yaml
```

## Trigger a health change

The demo workload lets you make pods temporarily unhealthy by sending `SIGUSR1` to the process.

```bash
POD1=$(kubectl get pods -l app=web-app -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$POD1" -- kill -USR1 1
```

Then observe the service annotation change:

```bash
kubectl get service web-app -o jsonpath='{.metadata.annotations.dcontroller\.io/pod-ready}'
```

When pods recover or restart, the annotation moves back toward the full ready count.

## What this example shows

The first circuit reduces pod state into a simple intermediate shape, and the second consumes
that reduced form instead of joining services directly with raw pod status. In this example the
intermediate `HealthView` is an internal stream, invisible outside the operator; turn it into a
view source/target pair when the intermediate state should be inspectable with kubectl.

## Cleanup

```bash
kubectl delete deployment web-app
kubectl delete service web-app
kubectl delete operator svc-health-operator
```
