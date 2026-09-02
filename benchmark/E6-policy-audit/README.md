# E6 - policy audit: dpolicy vs stock Gatekeeper

Does continuous, delta-driven policy auditing beat the periodic
state-of-the-world audit scan on the metrics that matter for security - 
detection latency, detection *completeness*, and idle cost? The contenders
audit the same inventory on the same envtest plant under the same
instrument: the stock Gatekeeper audit binary (built from the pinned
upstream tag by `make gk`) and the dpolicy controller (`apps/dpolicy`),
each running as its own process.

## Modes

- `dpolicy` - the dpolicy controller (reconciled, violation views off),
  spawned by run.sh against the plant's kubeconfig.
- `dpolicy-smith` - the same controller with `--mode smith`: the Smith
  dead-time compensator replaces the Reconciler on the status outputs
  (exactly-once actuation, watch echoes retired by content).
- `gk` - stock Gatekeeper, default audit architecture: every cycle
  re-LISTs the inventory from the API server and re-evaluates everything.
- `gk-cache` - Gatekeeper with `--audit-from-cache`: the inventory is
  replicated into its informer cache (the driver applies the `Config`
  sync object), removing the per-cycle fetch cost. Architecturally the
  closer relative of dpolicy's watch-maintained state - the remaining
  difference is the compute schedule (periodic full re-evaluation vs
  delta evaluation).

Gatekeeper runs `--operation=audit --operation=status --operation=generate`
(generate owns ConstraintTemplate -> CRD generation, upstream a
controller-manager duty), external data disabled (it demands webhook
certs), harness-assigned ports, and an on-disk API cache dir - see
`../plant/main.go` for the full invocation. Embedding Gatekeeper's
importable audit packages instead of running the stock binary was
considered and rejected: the binary is what users deploy, and its flags
plus the shared instruments below cover everything the workloads need.

## Workloads

- `w1` **detection latency** - one compliance flip against the converged
  base; flip-write -> first constraint status carrying the new count.
- `w2` **idle cost** - no churn for a window; status-write counts from the
  watch, CPU/RSS from the sampler, API load from the request log.
- `w3` **transient completeness** - violations living exactly L seconds;
  detected or missed. A violation shorter than the audit interval is
  architecturally invisible to a periodic scanner; a delta engine sees
  every one.
- `w4` **policy rollout** - one new constraint over the converged
  inventory; constraint-write -> first status with the full count (the
  everyone-pays-O(N) case; compare constants and queueing).
- `w5` **churn storm** - a violation ramp at a fixed event rate; per-event
  lag from the status-count crossings.

The `preload` row records baseline convergence (N pods written through the
driver, then first correct `totalViolations`); it includes the driver's own
write time, identical across modes.

## Instrumentation (symmetric by construction)

- **Driver clock + status watch**: every latency and completeness number is
  a content predicate on `Constraint.status.totalViolations`, timestamped
  at watch arrival in the driver - the same code path for every mode.
  Gatekeeper's own Prometheus metrics are deliberately not used (dpolicy
  has no equivalent; the one-sided instrument would poison the comparison).
- **`/proc` sampler** (`results/proc.csv`): per-second cumulative CPU ticks
  and RSS pages of the contender process, same sampler for all.
- **Client budgets, as shipped**: each contender runs its own default
  client-side rate limit - Gatekeeper configures none (client-go's
  5 QPS / burst 10), dpolicy's runtime defaults to 50/100. The driver is
  a load generator and runs at 500/1000 with the inventory sharded over
  parallel updaters (`../lib/loader.js`); driver throughput is identical
  across modes and does not touch either contender's budget.
- **API-server request log** (`/tmp/gk-serve/apiserver-requests.log`):
  the plant's own audit log, one Metadata-level JSON line per request,
  attributable per contender by user agent - the shared meter for W2's
  API-load comparison.

## Usage

```sh
cd .. && make envtest gk      # once per machine: k8s binaries + gatekeeper
make e6                       # fast (laptop) profile
make e6 SCALE=full            # paper-sized (hours; use a server)

# One point by hand:
MODES=gk NS=1000 PS=10 WORKLOADS=w1,w3 ./run.sh
```

Knobs (env): `MODES`, `NS`, `PS`, `REPS`, `WORKLOADS`, `AUDIT_INTERVAL`
(Gatekeeper's cycle, seconds), `IDLE_SECS`, `LIFETIMES`, `RATES`, `EVENTS`.

## Layout

- `bench.js` - the mode-agnostic driver (workloads above).
- `run.sh` - grid sweep; one fresh plant per (mode, N, P) point.
- `crds/` - the Gatekeeper-generated constraint CRD, vendored so the
  `dpolicy` mode (no Gatekeeper running) has the constraint kinds.
- `../plant/` - the shared plant: envtest + request-audit logging +
  the optional gatekeeper spawn.
- `../lib/polstack.js` - load writers, status taps, corpus fixtures.

## Corpus

`K8sRequiredLabels` (the stock required-labels template, byte-identical for
both engines - dpolicy evaluates the same Rego via `@rego`) with P
constraints matching pods in the bench namespace. Extending the corpus
with gatekeeper-library PSS checks (privileged, hostPath, capabilities) is
planned; referential policies (unique-ingress-host, NetworkPolicy
coverage) are a separate experiment.
