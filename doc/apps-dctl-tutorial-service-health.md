# Tutorial: Service Health Monitor

This example shows how to chain two declarative controllers through a local view. The result is a
small operator that annotates each `Service` with a ready-pod count such as `2/2`. The first
controller watches labeled pods and builds a `HealthView`. The second controller joins that view
with native `Service` objects and patches the result back onto the service.

The example files live in `dcontroller/examples/service-health-monitor/`.

## Apply the operator and workload

```bash
kubectl apply -f dcontroller/examples/service-health-monitor/svc-health-operator.yaml
kubectl apply -f dcontroller/examples/service-health-monitor/web-app.yaml
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

Generate a kubeconfig for the view API server and list resources:

```bash
dctl generate-config --http --user=dev --namespaces="*" \
  --server-address=localhost:8443 > /tmp/dctl.config
KUBECONFIG=/tmp/dctl.config kubectl api-resources
```

Then read the generated `HealthView` objects:

```bash
KUBECONFIG=/tmp/dctl.config kubectl get healthview.svc-health-operator.view.dcontroller.io -o yaml
```

This is the main reason the example is useful: it shows how a complex controller can be split into
two simpler ones with a visible intermediate state.

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

This example demonstrates the use of local views in the current repository. The first controller
reduces pod state into a simple intermediate object, and the second controller consumes that
reduced form instead of joining services directly with raw pod status.

## Cleanup

```bash
kubectl delete deployment web-app
kubectl delete service web-app
kubectl delete operator svc-health-operator
```
