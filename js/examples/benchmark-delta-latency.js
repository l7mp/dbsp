// Delta-latency benchmark for an incremental project pipeline.
//
// Usage: ./bin/dbsp examples/benchmark-delta-latency.js

const WARMUP_COUNT = 500;
const MEASURE_ROUNDS = 100;

aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "Pod",
  output: "output"
}).transform("Incrementalizer").validate();

let count = 0;
const iter = subscribe("output");

const warmupBatch = [];
for (let i = 0; i < WARMUP_COUNT; i++) {
  warmupBatch.push([{metadata: {name: `pod-${i}`, namespace: "default"}}, 1]);
}
publish("Pod", warmupBatch);

while (count < WARMUP_COUNT) {
  const step = await iter.next();
  if (step.done) {
    break;
  }
  count += step.value.length;
}
await iter.return();

console.log(`warmup: ${count} entries processed`);

const latencies = [];
for (let i = 0; i < MEASURE_ROUNDS; i++) {
  const t0 = performance.now();
  const done = subscribe.once("output");
  publish("Pod", [[{metadata: {name: `probe-${i}`, namespace: "default"}}, 1]]);
  await done;
  latencies.push(performance.now() - t0);
}

latencies.sort((a, b) => a - b);
const sum = latencies.reduce((a, b) => a + b, 0);
const avg = sum / latencies.length;
const p50 = latencies[Math.floor(latencies.length * 0.5)];
const p95 = latencies[Math.floor(latencies.length * 0.95)];
const p99 = latencies[Math.floor(latencies.length * 0.99)];

console.log(`rounds:  ${MEASURE_ROUNDS}`);
console.log(`avg:     ${avg.toFixed(3)} ms`);
console.log(`p50:     ${p50.toFixed(3)} ms`);
console.log(`p95:     ${p95.toFixed(3)} ms`);
console.log(`p99:     ${p99.toFixed(3)} ms`);
console.log(`min:     ${latencies[0].toFixed(3)} ms`);
console.log(`max:     ${latencies[latencies.length - 1].toFixed(3)} ms`);
exit();
