# Δ-controller Helm Chart

This chart deploys Δ-controller and its embedded API server.

## Quick start

```bash
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system \
  --create-namespace
```

Default install uses development mode (`apiServer.mode=development`): HTTP, authentication disabled,
and internal `ClusterIP` service.

## Documentation

For production mode (TLS Secret setup), authentication/authorization profiles, service exposure
options (`LoadBalancer`, `NodePort`, Gateway API), troubleshooting, and full configuration
guidance, see the [`doc/apps-dctl-getting-started.md`](/doc/apps-dctl-getting-started.md).

## License

Copyright 2026 by its authors. Some rights reserved. See [AUTHORS](/AUTHORS).

MIT License - see [LICENSE](/LICENSE) for full text.
