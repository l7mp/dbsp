// observer-demo.js
//
// Demonstrates observer APIs:
//   1) circuit.observe(fn)
//   2) runtime.observe(circuitName, fn)

runtime.onError((e) => {
  console.error(`[runtime:${e.origin}] ${e.message}`);
});

const c = aggregate.compile([
  { "@project": { "$.": "$." } }
], {
  inputs: "obs-input",
  outputs: ["obs-output"]
});

// Handle-scoped observer.
c.observe((e) => {
  const node = e.node.id;
  const pos = e.position;
  runtime.publish("observer-events", [[{ source: "circuit.observe", node, pos }, 1]]);
});

// Validate installs runtime circuit. Aggregate compiler uses circuit name "aggregation".
c.validate();

// Simple debug observer: dump payload directly.
runtime.observe("aggregation", (e) => {
  console.log("[agg-debug]", e);
});

subscribe("obs-output", (entries) => {
  for (const [doc, weight] of entries) {
    console.log(`[output ${weight}]`, doc);
  }
  exit();
});

publish("obs-input", [[{ id: 1, name: "alpha" }, 1]]);
