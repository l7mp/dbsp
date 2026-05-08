# Δ-controller Helm Chart

This chart deploys Δ-controller as a `dbsp` JavaScript runtime workload.

## Quick start

```bash
helm repo add dcontroller https://l7mp.github.io/dcontroller/
helm repo update
helm upgrade --install dcontroller dcontroller/dcontroller \
  --namespace dcontroller-system \
  --create-namespace
```

Default install uses development-oriented chart values. Runtime API-server
behavior is expected to be configured in the mounted JS script via
`kubernetes.runtime.config(...).start()`.
The default script path is `/dcontroller/dcontroller.js` inside the image.

For local utility commands without helper files, `dbsp` supports inline eval:

```bash
./js/bin/dbsp -e 'console.log(typeof kubernetes.runtime.config({}).generateKeys)'
```

## Documentation

For production mode (TLS Secret setup), authentication/authorization profiles, service exposure
options (`LoadBalancer`, `NodePort`, Gateway API), troubleshooting, and full configuration
guidance, see the [`doc/apps-dctl-getting-started.md`](/doc/apps-dctl-getting-started.md).

## License

Copyright 2026 by its authors. Some rights reserved. See [AUTHORS](/AUTHORS).

MIT License - see [LICENSE](/LICENSE) for full text.
