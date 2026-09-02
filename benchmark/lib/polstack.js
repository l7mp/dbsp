// The policy-audit benchmark driver surface (E6). The driver is the same
// for every contender and hosts none of them: the systems under test run as
// separate processes against the shared envtest plant (the stock
// gatekeeper binary, or the dpolicy controller spawned by run.sh), and this
// stack only writes the load and watches the constraint statuses - one
// clock, one watch, one content predicate for every mode.

const fs = require("fs");
const { Tap, sleep } = require("./dgwstack.js");
const { ShardedLoader } = require("./loader.js");
const fixtures = require("../../apps/dpolicy/lib/fixtures.js");

const TEMPLATE_GVK = "templates.gatekeeper.sh/v1/ConstraintTemplate";
const POD_GVK = "v1/Pod";
const NAMESPACE_GVK = "v1/Namespace";
const CONFIG_GVK = "config.gatekeeper.sh/v1alpha1/Config";

function constraintGVK(kind) {
  return `constraints.gatekeeper.sh/v1beta1/${kind}`;
}

// The audited world lives in one namespace; constraints match it
// explicitly, so the harness's own objects (the fake gatekeeper pod) never
// enter the counts.
const BENCH_NS = "bench-e6";

// The PSS corpus: three gatekeeper-library-shaped per-resource policies
// next to required-labels, so the audited policy set is heterogeneous
// rather than one toy check. The violating inventory pods trip all of
// them; c0 (required-labels) stays the measurement probe.
const PSS_TEMPLATES = [
  {
    kind: "K8sPSPPrivilegedContainer",
    schema: { type: "object" },
    rego: `
package k8spspprivileged

violation[{"msg": msg}] {
	c := input_containers[_]
	c.securityContext.privileged
	msg := sprintf("privileged container is not allowed: %v", [c.name])
}

input_containers[c] { c := input.review.object.spec.containers[_] }
input_containers[c] { c := input.review.object.spec.initContainers[_] }
`,
  },
  {
    kind: "K8sPSPHostFilesystem",
    schema: { type: "object" },
    rego: `
package k8spsphostfilesystem

violation[{"msg": msg}] {
	volume := input.review.object.spec.volumes[_]
	volume.hostPath
	msg := sprintf("hostPath volume is not allowed: %v", [volume.name])
}
`,
  },
  {
    kind: "K8sPSPCapabilities",
    schema: {
      type: "object",
      properties: {
        disallowedCapabilities: { type: "array", items: { type: "string" } },
      },
    },
    rego: `
package k8spspcapabilities

violation[{"msg": msg}] {
	c := input_containers[_]
	capability := c.securityContext.capabilities.add[_]
	capability == input.parameters.disallowedCapabilities[_]
	msg := sprintf("container %v has disallowed capability %v", [c.name, capability])
}

input_containers[c] { c := input.review.object.spec.containers[_] }
input_containers[c] { c := input.review.object.spec.initContainers[_] }
`,
  },
];

const PSS_KINDS = PSS_TEMPLATES.map((t) => t.kind);
const PSS_PARAMETERS = {
  K8sPSPCapabilities: { disallowedCapabilities: ["SYS_ADMIN"] },
};

class PolStack {
  // opts.handshake: path to the plant handshake JSON.
  // opts.pss: include the PSS policy corpus (default true).
  constructor(opts = {}) {
    this.pss = opts.pss !== false;
    this.constraintKinds = ["K8sRequiredLabels", ...(this.pss ? PSS_KINDS : [])];
    this.runtimeErrors = 0;
    this.lastActivity = performance.now();

    const hs = JSON.parse(
      fs.readFileSync(opts.handshake || "/tmp/gk-serve/handshake.json", "utf8"),
    );
    this.handshake = hs;

    // The driver is a load generator: give it a driver-sized client
    // budget (the contenders under test keep their own shipped defaults)
    // and a distinct user agent, so the API-server request log attributes
    // driver traffic separately from the dpolicy controller's (both are
    // dbsp binaries).
    kubernetes.runtime.start({
      kubeconfig: hs.kubeconfig,
      qps: 500,
      burst: 1000,
      userAgent: "e6-driver",
    });
    runtime.onError((err) => {
      this.runtimeErrors++;
      console.log(`RUNTIME ERROR: ${JSON.stringify(err)}`);
    });

    // Load writers: pods are the bulk inventory (sharded); the policy
    // objects are few and keep single consumers.
    kubernetes.update("bench.load.template", { gvk: TEMPLATE_GVK });
    kubernetes.update("bench.load.namespace", { gvk: NAMESPACE_GVK });
    kubernetes.update("bench.load.config", { gvk: CONFIG_GVK });
    this.podLoader = new ShardedLoader({ gvk: POD_GVK, prefix: "bench.load.pod", shards: 8 });
    this.constraintTopics = {};
    for (const kind of this.constraintKinds) {
      const topic = `bench.load.constraint.${kind.toLowerCase()}`;
      kubernetes.update(topic, { gvk: constraintGVK(kind) });
      this.constraintTopics[kind] = topic;
    }

    // The measurement surface: a watch tap per constraint kind, plus a
    // status-event trace (arrival time, constraint, totalViolations) the
    // workloads read back - detection timestamps, cycle structure, and
    // completeness all come from this one instrument.
    this.statusTaps = {};
    this.statusEvents = [];
    this.latestStatus = new Map();
    for (const kind of this.constraintKinds) {
      const topic = `bench.watch.constraint.${kind.toLowerCase()}`;
      kubernetes.watch(topic, { gvk: constraintGVK(kind) });
      this.statusTaps[kind] = new Tap(topic, this);
      subscribe(topic, (entries) => {
        const at = performance.now();
        for (const [doc, w] of entries) {
          if (w > 0 && doc.status && doc.status.totalViolations !== undefined) {
            const ev = {
              at,
              name: doc.metadata?.name,
              totalViolations: doc.status.totalViolations,
            };
            this.statusEvents.push(ev);
            this.latestStatus.set(ev.name, ev);
          }
        }
        return entries;
      });
    }
  }

  resetTrace() {
    this.statusEvents = [];
  }

  // --- corpus -----------------------------------------------------------

  // installCorpus applies the bench namespace, the templates and the
  // constraints: P required-labels constraints c0..c{P-1} (identical
  // params - every violating pod violates all of them; c0 is the probe, P
  // scales the per-cycle work), plus one constraint per PSS policy when
  // the PSS corpus is on.
  installCorpus(p) {
    publish("bench.load.namespace", [
      [{ apiVersion: "v1", kind: "Namespace", metadata: { name: BENCH_NS } }, 1],
    ]);
    publish("bench.load.template", [[fixtures.template(), 1]]);
    this.constraints = [];
    for (let i = 0; i < p; i++) {
      this.constraints.push(this.constraint(`c${i}`));
    }
    publish(
      this.constraintTopics.K8sRequiredLabels,
      this.constraints.map((c) => [c, 1]),
    );

    if (this.pss) {
      for (const t of PSS_TEMPLATES) {
        const template = fixtures.template(t.kind, t.rego);
        template.spec.crd.spec.validation = { openAPIV3Schema: t.schema };
        publish("bench.load.template", [[template, 1]]);
        publish(this.constraintTopics[t.kind], [
          [
            {
              apiVersion: "constraints.gatekeeper.sh/v1beta1",
              kind: t.kind,
              metadata: { name: `pss-${t.kind.toLowerCase()}` },
              spec: {
                match: {
                  namespaces: [BENCH_NS],
                  kinds: [{ apiGroups: [""], kinds: ["Pod"] }],
                },
                ...(PSS_PARAMETERS[t.kind] ? { parameters: PSS_PARAMETERS[t.kind] } : {}),
              },
            },
            1,
          ],
        ]);
      }
    }
  }

  constraint(name) {
    return fixtures.constraint(name, {
      labels: ["owner"],
      match: {
        namespaces: [BENCH_NS],
        kinds: [{ apiGroups: [""], kinds: ["Pod"] }],
      },
    });
  }

  // syncConfig makes gatekeeper replicate pods into its cache
  // (--audit-from-cache mode reads the inventory from there).
  applySyncConfig() {
    publish("bench.load.config", [
      [
        {
          apiVersion: "config.gatekeeper.sh/v1alpha1",
          kind: "Config",
          metadata: { name: "config", namespace: "gatekeeper-system" },
          spec: { sync: { syncOnly: [{ group: "", version: "v1", kind: "Pod" }] } },
        },
        1,
      ],
    ]);
  }

  // --- inventory ---------------------------------------------------------

  compliantPod(name) {
    return fixtures.pod(name, { namespace: BENCH_NS, labels: { owner: "bench", app: name } });
  }

  // installProbes creates K isolated probe slots for concurrent
  // transient-violation reps: each slot is its own namespace plus a
  // required-labels constraint scoped to it, so K probes run concurrently
  // with independent {0, 1} violation counts - no confounding through the
  // shared c0 count, and no disturbance of the base inventory.
  installProbes(k) {
    this.probeConstraints = [];
    const namespaces = [];
    for (let j = 0; j < k; j++) {
      namespaces.push([
        { apiVersion: "v1", kind: "Namespace", metadata: { name: `probe-${j}` } },
        1,
      ]);
      this.probeConstraints.push(
        fixtures.constraint(`c-probe-${j}`, {
          labels: ["owner"],
          match: {
            namespaces: [`probe-${j}`],
            kinds: [{ apiGroups: [""], kinds: ["Pod"] }],
          },
        }),
      );
    }
    publish("bench.load.namespace", namespaces);
    publish(
      this.constraintTopics.K8sRequiredLabels,
      this.probeConstraints.map((c) => [c, 1]),
    );
  }

  // removeProbes retracts the probe constraints. The namespaces stay:
  // envtest has no namespace controller, so a deleted namespace would hang
  // in Terminating; an empty namespace is harmless.
  removeProbes() {
    publish(
      this.constraintTopics.K8sRequiredLabels,
      this.probeConstraints.map((c) => [c, -1]),
    );
    this.probeConstraints = [];
  }

  // violatingPod trips every corpus policy: the owner label is missing
  // (required-labels), the container is privileged with a disallowed
  // capability, and a hostPath volume is mounted.
  violatingPod(name, namespace = BENCH_NS) {
    const pod = fixtures.pod(name, { namespace, labels: { app: name } });
    if (this.pss) {
      pod.spec.containers[0].securityContext = {
        privileged: true,
        capabilities: { add: ["SYS_ADMIN"] },
      };
      pod.spec.volumes = [{ name: "host", hostPath: { path: "/tmp" } }];
    }
    return pod;
  }

  // loadInventory creates N pods, nViolating of them violating, through
  // the sharded loader.
  loadInventory(n, nViolating) {
    const pods = [];
    for (let i = 0; i < n; i++) {
      pods.push(i < nViolating ? this.violatingPod(`base-v-${i}`) : this.compliantPod(`base-c-${i}`));
    }
    this.podLoader.write(pods);
  }

  addPod(pod) {
    this.podLoader.write([pod], 1);
  }

  removePod(pod) {
    this.podLoader.write([pod], -1);
  }

  // --- measurement -------------------------------------------------------

  // waitCount resolves with the arrival time of the first status event of
  // the named constraint carrying exactly the given totalViolations. The
  // current state is consulted first: with a fast contender the status can
  // land before the waiter registers (a tap waiter only sees future
  // events), and the recorded arrival time keeps the measurement exact.
  waitCount(name, count, timeoutMs, label) {
    const latest = this.latestStatus.get(name);
    if (latest && latest.totalViolations === count) {
      return Promise.resolve(latest.at);
    }
    return this.statusTaps.K8sRequiredLabels.waitMatch(
      (doc, w) =>
        w > 0 && doc.metadata?.name === name && doc.status?.totalViolations === count,
      timeoutMs,
      label,
    );
  }
}

module.exports = { PolStack, BENCH_NS, PSS_TEMPLATES, constraintGVK, sleep };
