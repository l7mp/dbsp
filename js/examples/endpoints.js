// endpoints.js -- In-memory Endpoints controller.

runtime.onError((e) => {
    console.error(`[runtime:${e.origin}] ${e.message}`);
});

aggregate.compile(
    [
        { "@join": [
            { "@eq": ["$.pods.metadata.labels.app", "$.services.spec.selector.app"] },
            { inputs: ["pods", "services"] },
        ]},
        { "@groupBy": ["$.services.metadata.name", "$.pods.status.podIP"] },
        { "@project": {
            apiVersion: "endpointsdemo.view.dcontroller.io/v1alpha1",
            kind:       "Endpoints",
            metadata:   { name: "$.key", namespace: "default" },
            endpoints:  "$.values",
        }},
    ], {
        inputs:  ["pods", "services"],
        outputs: ["endpoints"],
    }
).transform("Incrementalizer").validate();

subscribe("endpoints", (entries) => {
    console.log("==endpoints==");
    for (const [doc, weight] of entries) {
        console.log(JSON.stringify(doc), "->", weight > 0 ? `+${weight}` : weight);
    }
});

publish("pods", [
    [{ metadata: { name: "nginx-1", labels: { app: "web" } }, status: { podIP: "10.0.0.1" } }, 1],
    [{ metadata: { name: "nginx-2", labels: { app: "web" } }, status: { podIP: "10.0.0.2" } }, 1],
    [{ metadata: { name: "redis-1", labels: { app: "db"  } }, status: { podIP: "10.0.0.3" } }, 1],
]);

publish("services", [
    [{ metadata: { name: "web-svc" }, spec: { selector: { app: "web" } } }, 1],
    [{ metadata: { name: "db-svc"  }, spec: { selector: { app: "db"  } } }, 1],
]);

publish("pods", [
    [{ metadata: { name: "nginx-2", labels: { app: "web" } }, status: { podIP: "10.0.0.2" } }, -1],
    [{ metadata: { name: "nginx-2", labels: { app: "web" } }, status: { podIP: "10.0.0.4" } }, 1],
]);

setInterval(() => exit(), 1000);
