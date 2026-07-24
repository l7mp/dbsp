# Documentation

This is the technical documentation for the DBSP incremental computing engine and the DBSP
applications in this repository. 

- [What is DBSP?](/doc/what-is-dbsp.md)
- [Getting Started](/doc/getting-started.md)
- Concepts
  - [Overview: snapshot vs incremental computation](/doc/concepts-overview-snapshot-vs-incremental.md)
  - [Basics: documents, Z-sets, circuits, operators](/doc/concepts-basics-data-models-zsets-circuits-operators.md)
  - [Runtime: producers, consumers, processors, pub-sub](/doc/concepts-runtime-producers-consumers-processors.md)
  - [Programming: compilers and expressions](/doc/concepts-programming-compilers-and-expressions.md)
  - [Circuit transforms: Incrementalizer, Reconciler, SmithPredictor](/doc/concepts-transforms.md)
  - [JavaScript: writing and running DBSP programs](/doc/concepts-dbsp-javascript.md)
- Connectors
  - Kubernetes: watching and writing cluster state
    - [Overview](/doc/connectors-kubernetes.md)
    - [Extension API Server](/doc/connectors-kubernetes-API-server.md)
  - Envoy xDS: serving and consuming control-plane configuration
    - [Overview](/doc/connectors-xds.md)
- Applications
  - Δ-controller: A NoCode/LowCode incremental Kubernetes controller framework
    - [Overview](/doc/apps-dctl-overview.md)
    - [Getting Started](/doc/apps-dctl-getting-started.md)
    - [Sources, Targets, and Pipelines](/doc/apps-dctl-sources-targets-pipeline.md)
    - Tutorials
      - [ConfigMap to Deployment](/doc/apps-dctl-tutorial-configmap-deployment.md)
      - [Service Health Monitor](/doc/apps-dctl-tutorial-service-health.md)
      - [EndpointSlice Hybrid Consumer](/doc/apps-dctl-tutorial-endpointslice.md)

## License

Copyright 2026 by its authors. Some rights reserved. See [AUTHORS](/AUTHORS).

MIT License - see [LICENSE](/LICENSE) for full text.
