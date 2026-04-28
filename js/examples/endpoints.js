// endpoints.js -- In-memory Endpoints controller.

runtime.onError((e) => {
    console.error(`[runtime:${e.origin}] ${e.message}`);
});

aggregate.compile(
    [
        { "@join": { "@eq": ["$.pods.metadata.labels.app", "$.services.spec.selector.app"] } },
        { "@groupBy": ["$.services.metadata.name", "$.pods.status.podIP"] },
        { "@project": {
            metadata: {
                namespace: "$.key.namespace",
                name: "$.key.name",
            },
            endpoints: "$.values",
        }},
    ],
    {
        inputs: ["pods", "services"],
        outputs: ["endpoints"],
    }
).validate();

subscribe("endpoints", (entries) => {
    console.log("=== endpoints ===");
    for (const [doc, weight] of entries) {
        console.log(weight > 0 ? `+${weight}` : weight, JSON.stringify(doc));
    }
    exit();
});

const pods = [
    {
        kind: "Pod",
        metadata: { namespace: "default", name: "nginx-1", labels: { app: "web" } },
        status: { podIP: "10.0.0.1" },
    },
    {
        kind: "Pod",
        metadata: { namespace: "default", name: "nginx-2", labels: { app: "web" } },
        status: { podIP: "10.0.0.2" },
    },
    {
        kind: "Pod",
        metadata: { namespace: "default", name: "redis-1", labels: { app: "db" } },
        status: { podIP: "10.0.0.3" },
    },
];

const services = [
    {
        kind: "Service",
        metadata: { namespace: "default", name: "web-svc" },
        spec: { selector: { app: "web" } },
    },
    {
        kind: "Service",
        metadata: { namespace: "default", name: "db-svc" },
        spec: { selector: { app: "db" } },
    },
];

publish("pods", pods.map((doc) => [doc, 1]));
publish("services", services.map((doc) => [doc, 1]));

setInterval(() => exit(), 2000);
