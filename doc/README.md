# Documentation Index

This directory is the technical documentation for DBSP in this repository. Start with the short overview documents if you are new to the project, then move on to the concepts and application-specific guides as needed.

- [What is DBSP?](/doc/what-is-dbsp.md)
- [Getting Started](/doc/getting-started.md)
- Concepts
  - [Overview: snapshot vs incremental computation](/doc/concepts-overview-snapshot-vs-incremental.md)
  - [Basics: documents, Z-sets, circuits, operators](/doc/concepts-basics-data-models-zsets-circuits-operators.md)
  - [Runtime: producers, consumers, processors, pub-sub](/doc/concepts-runtime-producers-consumers-processors.md)
  - [Programming: compilers and expressions](/doc/concepts-programming-compilers-and-expressions.md)
- Applications
  - DBSP Script: JavaScript runtime for DBSP
    - [Overview and reference](/doc/apps-dbsp-script.md)
  - Δ-controller: A NoCode/LowCode incremental Kubernetes controller framework
    - [Overview](/doc/apps-dctl-overview.md)
    - [Getting Started](/doc/apps-dctl-getting-started.md)
    - [Sources, Targets, and Pipelines](/doc/apps-dctl-sources-targets-pipeline.md)
    - [Extension API Server](/doc/apps-dctl-extension-api-server.md)
    - Tutorials
      - [ConfigMap to Deployment](/doc/apps-dctl-tutorial-configmap-deployment.md)
      - [Service Health Monitor](/doc/apps-dctl-tutorial-service-health.md)
      - [EndpointSlice Hybrid Consumer](/doc/apps-dctl-tutorial-endpointslice.md)
