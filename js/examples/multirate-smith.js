// multirate-smith.js -- The dual-rate Smith-K loop, laid out by hand.
//
// The Smith dead-time compensator (doc/pres/smith.org) in its incremental
// form, with the compensation window retimed to a wall clock: commands
// enter the window on the event clock and leave it on a tick stream, so
// the retirement window is K_t ticks of wall time instead of K circuit
// steps. Event floods no longer age the window (no re-emission storms at
// any load), and a lost write is re-detected after K_t ticks regardless
// of how quiet the loop is. System equations (delta form):
//
//   dS = dist^Delta(dY + z^-1 U - E),   U = integral(dD - dS)
//
// where E is the release stream of a K_t-cell tick-domain delay line fed
// by z^-1 U. Each cell holds what entered until the next tick releases it:
//
//   H = integral(X - G),   G = (z^-1 H) x g
//
// with g the tick stream: one +1 assertion of an EMPTY document per tick.
// This is the circuit the DualRateSmith transform injects, laid out by
// hand node by node; in production the tick input is fed by the misc
// Tick source's pulse mode. The gate is a plain cartesian
// product: document Merge with an empty right side returns the left
// document content-identical, so the gated corrections need no
// projection, and nothing integrates g anywhere.
// Every block is a standard operator; integral() is realized as the
// plus/z^-1 loop and dist^Delta as its compiled form (z^-1, integrate,
// H_func), exactly as the incrementalizer emits it.
//
// The script drives the loop through four scenarios and checks the
// emission pattern of each: single-shot actuation with an echo, window
// shielding under an event flood, lost-write re-detection after K_t
// ticks, and immediate disturbance rejection.
//
// Run:  ./js/bin/dbsp js/examples/multirate-smith.js

const KT = 3; // window depth in ticks

const c = circuit.create("multirate-smith");
const inD = c.input("desired"); //  dD: desired-state deltas
const inY = c.input("observed"); // dY: watch feedback deltas
const inG = c.input("tick"); //     g: the wall clock, one empty +1 per tick
const outU = c.output("corrections");

// U = integral(dD - dS): the pending correction, realized as plus/z^-1.
const sub = c.node("linear_combination:[1,-1]", "sub"); // dD - dS
const u = c.node("plus", "u"); //                          U
const zu = c.node("delay", "zu"); //                       z^-1 U: the entry tap
c.edge(inD, sub, 0);
c.edge(sub, u, 0);
c.edge(zu, u, 1);
c.edge(u, zu, 0);
c.edge(u, outU, 0);

// dS = dist^Delta(dY + z^-1 U - E): the Smith prediction delta. The
// dist^Delta is the compiled incremental distinct: H(previous integral,
// current delta), the integral fed z^-1-then-integrate.
const win = c.node("linear_combination:[1,1,-1]", "win");
const dn = c.node("noop", "dn");
const dd = c.node("delay", "dd");
const di = c.node("integrate", "di");
const dh = c.node("H_func", "dh"); // dS
c.edge(inY, win, 0);
c.edge(zu, win, 1);
c.edge(win, dn, 0);
c.edge(dn, dd, 0);
c.edge(dd, di, 0);
c.edge(di, dh, 0);
c.edge(dn, dh, 1);
c.edge(dh, sub, 1);

// The tick-domain window: KT transfer cells. Cell i holds everything that
// entered since the last tick (H = integral(X - G), the plus/z^-1 loop)
// and releases it on the tick (G = z^-1 H gated by g). The chain is fed
// by the entry tap; the last release stream is the exit tap E.
let x = zu;
let release = null;
for (let i = 1; i <= KT; i++) {
  const a = c.node("linear_combination:[1,1,-1]", `a${i}`); // X + z^-1 H - G
  const za = c.node("delay", `za${i}`); //                     z^-1 H
  const g = c.node("cartesian", `g${i}`); //                   G = z^-1 H x g
  c.edge(x, a, 0);
  c.edge(za, a, 1);
  c.edge(g, a, 2);
  c.edge(a, za, 0);
  c.edge(za, g, 0);
  c.edge(inG, g, 1);
  x = g;
  release = g;
}
c.edge(release, win, 2); // E, negated in the window sum

c.commit();

// --- The driver: scripted event and tick steps. ----------------------------

const emitted = [];
subscribe("corrections", (es) => {
  for (const [d, w] of es) emitted.push([d.name, w]);
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const doc = (name) => ({ kind: "Endpoints", name: name });

let step = async (topic, entries) => {
  publish(topic, entries);
  await sleep(20);
};
const tick = () => step("tick", [[{}, 1]]);

// count returns the net emissions seen for one document name so far.
const count = (name) => emitted.filter(([n]) => n === name).length;

let failures = 0;
const check = (what, got, want) => {
  const ok = got === want;
  console.log(`${ok ? "ok  " : "FAIL"} ${what}: ${got} (want ${want})`);
  if (!ok) {
    failures++;
  }
};

async function run() {
  // 1. Single-shot actuation: desire E1, echo it back after one tick,
  //    then let the window drain. One emission, then silence: the echo
  //    collapses against the in-flight window inside the dist, and the
  //    confirmed command expires without resurfacing.
  await step("desired", [[doc("E1"), 1]]);
  await tick();
  await step("observed", [[doc("E1"), 1]]);
  for (let i = 0; i < 2 * KT; i++) {
    await tick();
  }
  check("E1 emissions after echo and full window drain", count("E1"), 1);

  // 2. Event-flood shielding: desire E2 (no echo yet), then flood the
  //    loop with unrelated desired-state events. Event steps do not age
  //    the tick-domain window, so E2 is not re-emitted by the flood.
  await step("desired", [[doc("E2"), 1]]);
  for (let i = 0; i < 8; i++) {
    await step("desired", [[doc(`F${i}`), 1]]);
    await step("observed", [[doc(`F${i}`), 1]]); // their echoes
  }
  check("E2 emissions under the event flood (no ticks)", count("E2"), 1);

  // 3. Lost write: E2's echo never arrives. K_t ticks release it from
  //    the window, the prediction drops it, and the loop re-emits.
  for (let i = 0; i <= KT; i++) {
    await tick();
  }
  check("E2 emissions after K_t ticks without an echo", count("E2"), 2);
  await step("observed", [[doc("E2"), 1]]); // the retry's echo lands
  for (let i = 0; i < 2 * KT; i++) {
    await tick();
  }
  check("E2 emissions once the retry is confirmed", count("E2"), 2);

  // 4. Disturbance rejection is event-clocked, not tick-clocked: tamper
  //    E1 away and the repair is emitted on the very next step, no tick
  //    needed; its echo then retires through the window as usual.
  const before = count("E1");
  await step("observed", [[doc("E1"), -1]]);
  check("E1 repair emitted at first sight of the tamper", count("E1"), before + 1);
  await tick();
  await step("observed", [[doc("E1"), 1]]);
  for (let i = 0; i < 2 * KT; i++) {
    await tick();
  }
  check("E1 emissions after the repair's echo", count("E1"), before + 1);

  console.log(failures === 0 ? "OK" : `${failures} MISMATCH(ES)`);
  exit(failures === 0 ? 0 : 1);
}

run().catch((e) => {
  console.log("ERROR", e);
  exit(1);
});
