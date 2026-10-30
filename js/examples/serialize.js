// Freeze a small operator into a spec file and load it back.
//
// An operator is a serialized runtime: its RuntimeSpec, the sources,
// circuits and targets. This example builds the spec in JS, writes it to a
// file, reads the file back and loads it into a fresh runtime with
// runtime.create, the same way a Go host loads an embedded spec. The spec
// carries no name: whoever loads it names the runtime.
//
// The operator has no bindings: its one circuit reads the "Person" stream
// and writes the "Adult" stream, which the example feeds and watches through
// the runtime handle. A real operator adds sources and targets to the spec.
//
// To deploy the spec with dcontroller, wrap it into an Operator CR:
//
//   { apiVersion: "dcontroller.io/v1alpha1", kind: "Operator",
//     metadata: { name: "adults" }, spec }
//
// Custom expression operators (expression.register) cannot be frozen: they
// live in the JS VM, not in the spec, so a frozen operator may use only the
// built-in and the Go-backed operators.
//
// Run: ./js/bin/dbsp js/examples/serialize.js [file]

const fs = require("fs");

const file = argv[0] || "/tmp/adults-spec.yaml";

// Build: the spec.
const spec = {
    circuits: [{
        name: "adults",
        inputs: ["Person"],
        outputs: ["Adult"],
        pipeline: [
            { "@select": { "@gte": ["$.age", 18] } },
            { "@project": { name: "$.name", age: "$.age" } },
        ],
        transforms: [{ name: "Incrementalizer" }],
    }],
};

// Freeze: format.toYAML serializes each document into its "line" field.
const [[{ line }]] = format.toYAML([[spec, 1]]);
fs.writeFileSync(file, line);
console.log(`froze the spec into ${file}`);

// Load: parse the file and hand the spec to a fresh runtime, named here.
const [[loaded]] = format.yaml([[{ line: fs.readFileSync(file, "utf8") }, 1]]);
const handle = runtime.create("adults", loaded);
handle.start();
console.log(`loaded the spec: ${loaded.circuits.length} circuit(s)`);

let adults = 0;
handle.subscribe("Adult", (entries) => {
    for (const [doc, weight] of entries) {
        console.log(weight > 0 ? `+${weight}` : weight, JSON.stringify(doc));
        adults += weight;
    }
    if (adults === 2) {
        handle.close();
        exit();
    }
});

handle.publish("Person", [
    [{ name: "alice", age: 34 }, 1],
    [{ name: "bob", age: 12 }, 1],
    [{ name: "carol", age: 18 }, 1],
]);
