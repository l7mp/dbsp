# Tutorial: ConfigMap to Deployment

This example shows a purely declarative operator that restarts a `Deployment` when a referenced
`ConfigMap` changes. It is a compact example of a multi-source join plus a patch target.

The operator watches three resources:

- the `ConfigMap` that carries the data,
- the `Deployment` that should roll when the data changes,
- and a custom `ConfigDeployment` resource that links the two by name.

The pipeline joins those objects by name and namespace, then writes the current ConfigMap resource
version into the deployment pod template annotations. That small pod template change is enough to
trigger a rollout.

The example files live in `dcontroller/examples/configmap-deployment-controller/`.

## Apply the CRD and operator

```bash
kubectl apply -f dcontroller/examples/configmap-deployment-controller/configdeployment-crd.yaml
kubectl apply -f dcontroller/examples/configmap-deployment-controller/configdeployment-operator.yaml
```

The operator itself is named `configdep-operator` in the current example manifest.

## Create test objects

```bash
kubectl create configmap config --from-literal=key1=value1 --from-literal=key2=value2
kubectl create deployment dep --image=docker.io/l7mp/net-debug --replicas=2
kubectl apply -f - <<'EOF'
apiVersion: dcontroller.io/v1alpha1
kind: ConfigDeployment
metadata:
  name: dep-config
spec:
  configMap: config
  deployment: dep
EOF
```

## Verify the operator

Check that the operator reports readiness:

```bash
kubectl get operator configdep-operator -o yaml
```

Look for a `Ready` condition under `status.conditions`.

Now inspect the annotation written into the deployment template:

```bash
kubectl get deployment dep -o jsonpath='{.spec.template.metadata.annotations.dcontroller\.io/configmap-version}'
kubectl get configmap config -o jsonpath='{.metadata.resourceVersion}'
```

The two values should match.

## Trigger a rollout

Update the ConfigMap:

```bash
kubectl patch configmap/config --type merge -p '{"data":{"key3":"value3"}}'
```

Then watch the deployment roll and verify the annotation changes:

```bash
kubectl rollout status deployment/dep
kubectl get deployment dep -o jsonpath='{.spec.template.metadata.annotations.dcontroller\.io/configmap-version}'
kubectl get configmap config -o jsonpath='{.metadata.resourceVersion}'
```

## What this example shows

The important part is the operator shape, not the custom resource itself. The controller uses three
sources, starts with `@join`, and writes to a `Patcher` target on the deployment. This is a good
pattern whenever a declarative relation between objects should result in a small patch on an
existing native resource.

## Cleanup

```bash
kubectl delete configdeployment dep-config
kubectl delete deployment dep
kubectl delete configmap config
kubectl delete operator configdep-operator
kubectl delete crd configdeployments.dcontroller.io
```
