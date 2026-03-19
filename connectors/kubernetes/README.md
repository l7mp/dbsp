# connectors/kubernetes

Kubernetes connectors for DBSP runtime.

- `runtime/`: shared base layer (`store`, `apiserver`, `object`, `predicate`, `api/view`).
- `producer/`: Kubernetes watch source -> DBSP input.
- `consumer/`: DBSP output -> Kubernetes writes.

Non-Kubernetes trigger producers live in `connectors/misc`.
