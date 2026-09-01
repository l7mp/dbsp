# Δ-controller Overview

Δ-controller is the Kubernetes application layer of this repository. It takes the generic DBSP
machinery described in the main concepts guides and packages them up as Kubernetes controllers that
can react to object deltas instead of rebuilding state from scratch on every event. Note that you
can also write Kubernetes controllers via DBSP's JavaScript runtime. Δ-controller however exposes
controllers as a custom Kubernetes resource and manages the lifecycle, which makes it possible to
dynamically inject new controllers by a simple `kubectl apply`.

The main unit of deployment is an `Operator` custom resource: a frozen DBSP runtime. Its spec is
the engine's serialized runtime format verbatim, three sets coupled by named streams: sources
feeding streams, circuits processing them, and targets consuming them. Sources and targets can be
regular Kubernetes resources or local views that exist only inside Δ-controller, and streams
between circuits are internal wires. Each operator runs as its own private DBSP runtime.

```mermaid
flowchart LR
    A["Sources\nKubernetes resources or views"] --> B["Pipeline\njoin, select, project, groupBy"]
    B --> C["Targets\nKubernetes resources or views"]
```

This matters in the larger DBSP context because the controller logic is still just an incremental
dataflow. The same runtime can connect Kubernetes watches to DBSP circuits, but it can also join
those flows with other producers and consumers from the workspace. In practice that means a
pipeline may start from Kubernetes objects, pass through views, and end in native Kubernetes
resources, or it may feed another runtime consumer implemented in Go.

The main benefit is correctness by construction. A declarative circuit describes the snapshot
shape of the computation, and its transform chain says how it runs: with the `Incrementalizer` it
compiles into the incremental form that processes changes, and an empty chain means snapshot
execution over the full state. This avoids much of the usual operator boilerplate around watch
management, object joins, caching, and diff handling. The `Reconciler` transform additionally
closes a desired-state control loop that cancels external noise (an adversary rewriting the
target object heals), and the `Distincter` keeps outputs set-valued. Transforms are stated
explicitly per circuit; there are no defaults.

There are also deliberate tradeoffs. Δ-controller operates on unstructured objects, so there is no
compile-time schema safety. Views are in-memory only, so they disappear on restart and get rebuilt
from source watches. The framework is strongest when most of the work is data reshaping, joining,
filtering, and aggregation. If the last step must call an imperative API, Δ-controller can still be
used for the data preparation stage and hand the result to custom Go code.
