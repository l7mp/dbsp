"use strict";

const assert = require("assert");
const { setTimeout: promiseTimeout } = require("timers/promises");

// sleep returns a Promise that resolves after ms milliseconds.
function sleep(ms) {
    return promiseTimeout(ms);
}

// run iterates cases, calling runOne(tc) for each. Returns a Promise that
// resolves when all cases pass, or rejects on the first failure set.
function run(cases, runOne) {
    return new Promise((resolve, reject) => {
        setTimeout(async () => {
            let passed = 0;
            const failed = [];
            for (let i = 0; i < cases.length; i++) {
                const tc = cases[i];
                const name = tc.name != null ? String(tc.name) : `case-${i}`;
                try {
                    await runOne(tc);
                    passed++;
                    console.log(`[PASS] ${name}`);
                } catch (err) {
                    const msg = (err && err.message) ? err.message : String(err);
                    failed.push(`[FAIL] ${name}: ${msg}`);
                }
            }
            console.log(`\n${passed}/${cases.length} tests passed`);
            if (failed.length > 0) {
                for (const f of failed) {
                    console.log(f);
                }
                reject(new Error(`test run failed: ${failed.length} failures`));
                return;
            }
            resolve();
        }, 0);
    });
}

// describe registers a named test suite. fn receives an it(name, testFn)
// collector. All suites are run sequentially after the current event loop tick.
//
// Example:
//   describe("my module", (it) => {
//     it("does something", () => { assert.strictEqual(1, 1); });
//   });
function describe(suiteName, fn) {
    const cases = [];
    fn((name, testFn) => {
        cases.push({ name: `${suiteName}: ${name}`, fn: testFn });
    });

    const p = run(cases, (tc) => tc.fn());
    // Surface top-level failures so the VM exits non-zero.
    p.catch((err) => {
        console.error(`suite "${suiteName}" failed: ${err.message}`);
        // Re-throw to propagate through the event loop's unhandled rejection
        // handler, which causes the VM to exit with a non-zero code.
        throw err;
    });
}

module.exports = { assert, sleep, run, describe };
