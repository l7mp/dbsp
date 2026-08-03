# ConfigMap to Deployment

This example shows a purely declarative Δ-controller operator that watches a `ConfigMap`, a
`Deployment`, and a custom `ConfigDeployment` resource, then patches the deployment pod template
when the referenced ConfigMap changes.

The maintained tutorial is in [`doc/apps-dctl-tutorial-configmap-deployment.md`](/doc/apps-dctl-tutorial-configmap-deployment.md).
