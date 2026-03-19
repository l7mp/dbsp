# connectors/kubernetes

Kubernetes connectors for the DBSP runtime.

- `runtime/`: shared base layer (`store`, `apiserver`, `object`, `predicate`, `api/view`).
- `producer/`: Kubernetes watch source -> DBSP input.
- `consumer/`: DBSP output -> Kubernetes writes.
