# Tutorial: EndpointSlice Controller

This example shows two circuits chained through an internal stream, ending in a materialized
view. The `service-controller` circuit watches annotated `Service` objects, filters the ones
opted into endpoint processing, and expands service ports onto the internal `ServiceView` stream.
The `endpointslice-controller` circuit joins that stream with native `EndpointSlice` objects,
keeps only ready endpoints, and reshapes them into `EndpointView` objects written to a view
target, one per endpoint address (the flat variant) or one per service port with the address list
gathered (the gathered variant).

The example files live in `apps/dcontroller/examples/endpointslice-controller/`: the deployable
operator manifest (`endpointslice-operator.yaml`, the flat variant), the two bare runtime specs
(`endpointslice-controller-spec.yaml` and `-gather-spec.yaml`), and the self-contained test
scripts.

## Apply the operator

```bash
kubectl apply -f apps/dcontroller/examples/endpointslice-controller/endpointslice-operator.yaml
```

Because `EndpointView` is a view target, the generated objects are visible through the embedded
API server (see the service-health tutorial for the kubeconfig workflow):

```bash
KUBECONFIG=/tmp/dcontroller.config kubectl get endpointview.ep-operator.view.dcontroller.io -A
```

## Create test resources

Start with a simple deployment:

```bash
kubectl create deployment testdep --image=registry.k8s.io/pause:3.9 --replicas=2
```

Expose it with the opt-in annotation used by the example:

```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Service
metadata:
  name: testsvc
  annotations:
    dcontroller.io/endpointslice-controller-enabled: "true"
spec:
  selector:
    app: testdep
  ports:
    - name: http
      protocol: TCP
      port: 80
    - name: https
      protocol: TCP
      port: 8843
EOF
```

As EndpointSlices appear, the example binary logs the generated `EndpointView` deltas.

Scale the deployment to create more endpoints:

```bash
kubectl scale deployment testdep --replicas=3
```

Delete the service to observe delete events:

```bash
kubectl delete service testsvc
```

## What this example shows

Two circuits meet on an internal stream that is bound to nothing, and only the final result is
materialized as a view. The `ServiceView` stream never touches the API server: it is a wire
between circuits inside the operator's private runtime. Compare the flat and gathered specs to
see the same computation shaped by `@unwind` alone versus `@unwind` plus `@groupBy`.

## Cleanup

```bash
kubectl delete deployment testdep
kubectl delete service testsvc --ignore-not-found
```
