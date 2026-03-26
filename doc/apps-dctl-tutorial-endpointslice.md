# Tutorial: EndpointSlice Hybrid Consumer

This example is different from the others. It uses declarative Δ-controller pipelines to do the
difficult Kubernetes-side data preparation, but instead of writing the final result back to the API
server it consumes the resulting view deltas in Go. That makes it a good example of how
Δ-controller fits into the broader DBSP runtime. Kubernetes watches are only the inputs. The output
can just as well feed another consumer that programs some external system or another in-process
component.

The example files live in `dcontroller/examples/endpointslice-controller/`.

## What the example does

The example has two declarative controllers. The `service-controller` watches annotated `Service`
objects, filters the ones opted into endpoint processing, and expands service ports into a local
`ServiceView`. The `endpointslice-controller` joins `ServiceView` with native `EndpointSlice`
objects, keeps only ready endpoints, reshapes them into a compact `EndpointView`, and publishes the
resulting deltas.

The Go example binary then subscribes to those `EndpointView` deltas and logs add, update, and
delete events.

## Run the example

From the workspace root:

```bash
go run ./dcontroller/examples/endpointslice-controller
```

In the current implementation, the default mode groups addresses by service port, so one view
object contains a list of addresses. To get one object per endpoint address, disable pooling:

```bash
go run ./dcontroller/examples/endpointslice-controller --disable-endpoint-pooling
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

This example demonstrates the hybrid style supported by DBSP. The declarative controllers still do
the joins, filtering, unwinding, and grouping, but the final consumer is Go code running on the
same runtime rather than a Kubernetes target resource.

## Cleanup

```bash
kubectl delete deployment testdep
kubectl delete service testsvc --ignore-not-found
```
