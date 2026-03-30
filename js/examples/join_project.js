// Hypothetical JS rewrite of join_project.dbsp.
//
// Globals injected by the Go/goja host:
//   sql, aggregate                   - compilers
//   publish(topic, entries)          - generic Z-set publisher
//   producer.kubernetes.watch(opts)  - K8s watch producer
//   subscribe(topic, fn)             - generic callback subscriber
//   consumer.kubernetes.patcher(opts)
//   consumer.kubernetes.updater(opts)
//   runtime.* aliases for all calls + runtime.onError(fn)

runtime.onError((e) => {
    console.error(`[runtime:${e.origin}] ${e.message}`);
});

// === Schema ===
sql.table("products", "pid INT PRIMARY KEY, name TEXT, price FLOAT");
sql.table("orders",   "oid INT PRIMARY KEY, product_id INT, qty INT");

// === SQL incremental join circuit ===
const c = sql.compile(
    `SELECT oid, product_id, pid, name, price, qty
       FROM orders JOIN products ON product_id = pid`,
    { output: "joined-orders" }
).transform("Incrementalizer").validate();

console.log("circuit:", c);

// Fires once when products are published (join is empty — no orders yet),
// then again when orders are published (with joined rows).
subscribe("joined-orders", (entries) => {
    console.log("=== sql join output ===");
    for (const [doc, weight] of entries) {
        console.log(weight > 0 ? `+${weight}` : weight, doc);
    }
});

// === Aggregate incremental join circuit ===
aggregate.compile(
    [
        { "@join":    { "@eq": ["$.products.pid", "$.orders.product_id"] } },
        { "@project": {
            oid:        "$.orders.oid",
            product_id: "$.orders.product_id",
            pid:        "$.products.pid",
            name:       "$.products.name",
            price:      "$.products.price",
            qty:        "$.orders.qty",
        }},
    ],
    { inputs: ["orders", "products"], output: "joined-orders-agg" }
).transform("Incrementalizer");

subscribe("joined-orders-agg", (entries) => {
    console.log("=== aggregate join output ===");
    for (const [doc, weight] of entries) {
        console.log(weight > 0 ? `+${weight}` : weight, doc);
    }
});

// === Publish inputs ===
publish("products", [
    [{ pid: 1, name: "Widget", price: 9.99  }, 1],
    [{ pid: 2, name: "Gadget", price: 24.99 }, 1],
]);

const orderRows = [
    { oid: 101, product_id: 1, qty: 3 },
    { oid: 102, product_id: 2, qty: 1 },
];
publish("orders", orderRows.map((doc) => [doc, 1]));
