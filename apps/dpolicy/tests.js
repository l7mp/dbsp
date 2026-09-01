// Self-contained test suite for the dpolicy auditor.
//
// The pipeline is driven entirely through topics: Gatekeeper policy objects
// and pods are published to the input topics, constraint status documents
// are asserted on the per-kind status topics, and the violation/rejection
// log streams are asserted the same way.
//
// Run from the repo root:  ./js/bin/dbsp apps/dpolicy/index.js test

const { describe, assert } = require("testing");
const minimist = require("minimist");
const { compilePipeline } = require("./lib/pipeline.js");
const { use, collector, byName, upsert, retract, replace } = require("./lib/testlib.js");
const fixtures = require("./lib/fixtures.js");

// The suite runs the pipeline in either execution mode: the default
// incremental one, or --mode sotw, the jacketed snapshot execution. The
// cases are identical: the two compilations are the same semantics.
const argv = minimist(process.argv.slice(2));
const SOTW = String(argv.mode || "") === "sotw";

const CONSTRAINT_KINDS = ["K8sRequiredLabels", "K8sAlwaysBroken"];
const VIOLATION_LIMIT = 3;

function setup() {
  const pipeline = compilePipeline({
    constraintKinds: CONSTRAINT_KINDS,
    violationLimit: VIOLATION_LIMIT,
    violationViews: true,
    sotw: SOTW,
  });
  use(pipeline.handle);
  const topics = pipeline.topics;

  upsert(topics.inputs.template, fixtures.template());

  return {
    topics,
    status: collector(topics.status.K8sRequiredLabels),
    statusBroken: collector(topics.status.K8sAlwaysBroken),
    views: collector(topics.views.violation),
    violations: collector(topics.log.violation),
    rejections: collector(topics.log.rejection),
    // violationsFor filters the integrated violation log for one constraint.
    violationsFor(name) {
      return this.violations.docs().filter((d) => d.constraint.name === name);
    },
  };
}

const c = setup();
const T = c.topics;

describe("dpolicy", (it) => {
  it("reports all-clear for a compliant pod", async () => {
    upsert(T.inputs.constraint, fixtures.constraint("c1", { match: { namespaces: ["t1"] } }));
    upsert(T.inputs.pod, fixtures.pod("ok", { namespace: "t1", labels: { owner: "me" } }));

    const docs = await c.status.waitFor((d) => byName(d, "c1"), "c1 status");
    const st = byName(docs, "c1").status;
    assert.strictEqual(st.totalViolations, 0);
    assert.deepStrictEqual(st.violations, []);
  });

  it("flags a violating pod and logs it", async () => {
    upsert(T.inputs.pod, fixtures.pod("bad", { namespace: "t1" }));

    const docs = await c.status.waitFor(
      (d) => byName(d, "c1")?.status.totalViolations === 1,
      "c1 one violation",
    );
    const v = byName(docs, "c1").status.violations[0];
    assert.strictEqual(v.kind, "Pod");
    assert.strictEqual(v.group, "");
    assert.strictEqual(v.version, "v1");
    assert.strictEqual(v.name, "bad");
    assert.strictEqual(v.namespace, "t1");
    assert.strictEqual(v.enforcementAction, "deny");
    assert.ok(v.message.includes("owner"), `message: ${v.message}`);

    await c.violations.waitFor(() => c.violationsFor("c1").length === 1, "c1 log row");
  });

  it("materializes the violation as a view object", async () => {
    const docs = await c.views.waitFor((d) => d.length === 1, "one violation view");
    const view = docs[0];
    assert.strictEqual(view.kind, "Violation");
    assert.strictEqual(view.metadata.namespace, "t1");
    assert.ok(view.metadata.name.startsWith("c1-bad-"), view.metadata.name);
    assert.strictEqual(view.spec.constraint.name, "c1");
    assert.strictEqual(view.spec.violation.name, "bad");
  });

  it("clears the violation when the pod is fixed", async () => {
    replace(
      T.inputs.pod,
      fixtures.pod("bad", { namespace: "t1" }),
      fixtures.pod("bad", { namespace: "t1", labels: { owner: "me" } }),
    );

    await c.status.waitForQuiet(
      (d) => byName(d, "c1")?.status.totalViolations === 0,
      "c1 clean again",
    );
    assert.strictEqual(c.violationsFor("c1").length, 0);
    assert.strictEqual(c.views.docs().length, 0);
  });

  it("matches by labelSelector", async () => {
    upsert(
      T.inputs.constraint,
      fixtures.constraint("c4", {
        match: { namespaces: ["t4"], labelSelector: { matchLabels: { app: "web" } } },
      }),
    );
    upsert(T.inputs.pod, fixtures.pod("selected", { namespace: "t4", labels: { app: "web" } }));
    upsert(T.inputs.pod, fixtures.pod("unselected", { namespace: "t4" }));

    const docs = await c.status.waitForQuiet(
      (d) => byName(d, "c4")?.status.totalViolations === 1,
      "c4 selects one pod",
    );
    assert.strictEqual(byName(docs, "c4").status.violations[0].name, "selected");
  });

  it("ignores kinds the constraint does not match", async () => {
    upsert(
      T.inputs.constraint,
      fixtures.constraint("c5", {
        match: { namespaces: ["t5"], kinds: [{ apiGroups: ["apps"], kinds: ["Deployment"] }] },
      }),
    );
    upsert(T.inputs.pod, fixtures.pod("ignored", { namespace: "t5" }));

    await c.status.waitForQuiet(
      (d) => byName(d, "c5")?.status.totalViolations === 0,
      "c5 stays clean",
    );
  });

  it("respects namespace globs and excludedNamespaces", async () => {
    upsert(
      T.inputs.constraint,
      fixtures.constraint("c6", {
        match: { namespaces: ["t6*"], excludedNamespaces: ["t6-skip*"] },
      }),
    );
    upsert(T.inputs.pod, fixtures.pod("counted", { namespace: "t6-a" }));
    upsert(T.inputs.pod, fixtures.pod("skipped", { namespace: "t6-skip-1" }));

    const docs = await c.status.waitForQuiet(
      (d) => byName(d, "c6")?.status.totalViolations === 1,
      "c6 audits t6-a only",
    );
    assert.strictEqual(byName(docs, "c6").status.violations[0].name, "counted");
  });

  it("caps the violations list, keeps the full count", async () => {
    upsert(T.inputs.constraint, fixtures.constraint("c7", { match: { namespaces: ["t7"] } }));
    for (let i = 0; i < 5; i++) {
      upsert(T.inputs.pod, fixtures.pod(`bad-${i}`, { namespace: "t7" }));
    }

    const docs = await c.status.waitFor(
      (d) => byName(d, "c7")?.status.totalViolations === 5,
      "c7 counts all five",
    );
    assert.strictEqual(byName(docs, "c7").status.violations.length, VIOLATION_LIMIT);
    assert.strictEqual(c.violationsFor("c7").length, 5);
  });

  it("rejects a constraint with an unsupported match field", async () => {
    upsert(
      T.inputs.constraint,
      fixtures.constraint("c8", {
        match: {
          namespaces: ["t8"],
          namespaceSelector: { matchLabels: { audited: "true" } },
        },
      }),
    );
    upsert(T.inputs.pod, fixtures.pod("unaudited", { namespace: "t8" }));

    const rejections = await c.rejections.waitFor(
      (d) => d.some((r) => r.constraint.name === "c8"),
      "c8 rejected",
    );
    const rejection = rejections.find((r) => r.constraint.name === "c8");
    assert.ok(rejection.error.includes("namespaceSelector"), rejection.error);

    // The rejected constraint audits nothing, loudly - never wrongly.
    const docs = await c.status.waitForQuiet(
      (d) => byName(d, "c8")?.status.totalViolations === 0,
      "c8 audits nothing",
    );
    assert.deepStrictEqual(byName(docs, "c8").status.violations, []);
  });

  it("rejects a broken template", async () => {
    upsert(T.inputs.template, fixtures.template("K8sAlwaysBroken", "this is not rego"));
    upsert(T.inputs.constraint, fixtures.constraint("c9", { kind: "K8sAlwaysBroken" }));

    const rejections = await c.rejections.waitFor(
      (d) => d.some((r) => r.constraint.name === "c9"),
      "c9 rejected",
    );
    const rejection = rejections.find((r) => r.constraint.name === "c9");
    assert.ok(rejection.error.includes("invalid template"), rejection.error);

    await c.statusBroken.waitFor(
      (d) => byName(d, "c9")?.status.totalViolations === 0,
      "c9 empty status",
    );
  });

  it("propagates a template edit", async () => {
    upsert(T.inputs.constraint, fixtures.constraint("c10", { match: { namespaces: ["t10"] } }));
    upsert(T.inputs.pod, fixtures.pod("stale", { namespace: "t10" }));

    await c.status.waitFor(
      (d) => byName(d, "c10")?.status.violations[0]?.message.includes("you must provide"),
      "c10 v1 message",
    );

    replace(
      T.inputs.template,
      fixtures.template(),
      fixtures.template("K8sRequiredLabels", fixtures.REQUIRED_LABELS_REGO_V2),
    );

    await c.status.waitFor(
      (d) => byName(d, "c10")?.status.violations[0]?.message.includes("missing mandatory"),
      "c10 v2 message",
    );
  });

  it("retracts the status when the constraint goes away", async () => {
    retract(T.inputs.constraint, fixtures.constraint("c10", { match: { namespaces: ["t10"] } }));

    await c.status.waitForQuiet((d) => !byName(d, "c10"), "c10 status gone");
    assert.strictEqual(c.violationsFor("c10").length, 0);
  });
}).then(
  () => exit(0),
  () => exit(1),
);
