// handcraft.js -- Building a circuit node by node with the circuit.* API.
//
// aggregate.compile / sql.compile are the usual way to get a circuit, but
// sometimes you want to lay one out by hand -- to prototype a transform, to
// wire a shape the compiler does not emit, or just to see the operators. The
// circuit.* builder does exactly that. Every node is an operator named by a
// spec string (or a wire object); there are no per-operator constructors:
//
//   "delay", "plus", "distinct", ...          type-only structural/primitive ops
//   "linear_combination:[1,-1]"               ":arg" fills the op's argument
//   '@project:{"m":"$.n"}'                     the @-vocabulary, one op per node
//   { type: "select", predicate: {...} }      the full wire object
//
// Building a circuit never installs it: .commit() is the one call that
// validates (every cycle needs a delay) and hands it to the runtime, exactly
// as it does for a compiled circuit. Afterwards the handle is an ordinary
// circuit: .transform(), .observe(), .close() all work.
//
// Run:  ./js/bin/dbsp js/examples/handcraft.js

// --- A running integral, by hand: out = accumulate(in). --------------------
// The accumulator is a Plus fed by its own delayed output; the cycle
// acc -> z -> acc carries the state, and the delay is what makes it valid.
const c = circuit.create("accumulator");
const inD = c.input("delta"); // input_delta, bound to topic "delta"
const outS = c.output("sum"); // output_sum,   bound to topic "sum"
const acc = c.node("plus", "acc");
const z = c.node("delay", "z");
c.edge(inD, acc, 0); //   δ  ->  acc[0]
c.edge(z, acc, 1); //   z⁻¹ ->  acc[1]
c.edge(acc, z, 0); //   acc ->  z⁻¹
c.edge(acc, outS, 0); //   acc ->  out
c.commit(); // validates the cycle-has-a-delay rule, then installs

let running = null;
c.observe((step) => {
  // .observe taps the whole step; here we just watch the acc node's value.
  const v = step.values["acc"];
  if (v && v.length) running = v;
});

const sum = [];
subscribe("sum", (es) => {
  for (const [, w] of es) sum.push(w);
});

// --- A projection node from the @-vocabulary, in a second circuit. ---------
const p = circuit.create("projector");
const pin = p.input("raw");
const proj = p.node('@project:{"kind":"Point","x":"$.x","y":"$.y"}');
const pout = p.output("points");
p.edge(pin, proj, 0);
p.edge(proj, pout, 0);
p.commit();

const points = [];
subscribe("points", (es) => {
  for (const [d, w] of es) points.push([d.kind, d.x, d.y, w]);
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function run() {
  // Feed the accumulator three unit deltas for the SAME key; the integral's
  // weight for that key climbs 1, 2, 3 and the output re-emits it each step.
  for (let i = 0; i < 3; i++) {
    publish("delta", [[{ key: "x" }, 1]]);
    await sleep(20);
  }
  // Project one raw record.
  publish("raw", [[{ x: 3, y: 4 }, 1]]);
  await sleep(40);

  console.log("running integral (output weights):", JSON.stringify(sum));
  console.log("acc node observed non-empty:", running !== null);
  console.log("projected:", JSON.stringify(points));

  const ok =
    JSON.stringify(sum) === JSON.stringify([1, 2, 3]) &&
    points.length === 1 &&
    points[0][0] === "Point" &&
    points[0][1] === 3 &&
    points[0][2] === 4;
  console.log(ok ? "OK" : "MISMATCH");
  exit(ok ? 0 : 1);
}

run().catch((e) => {
  console.log("ERROR", e);
  exit(1);
});
