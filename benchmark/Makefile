# Driver for the benchmark suite. Run from this directory.
#
#   make build     build the dbsp CLI the benchmarks run on (js/bin/dbsp)
#   make prep      one-shot provisioning: build dbsp, download the envtest
#                  control-plane binaries, clone+build every contender
#                  (envoy-gateway serve binary, istiod, gatekeeper) and the
#                  plant launcher  -  run once per machine, then make full
#   make all       run E1, E2, E3, E5, E6 with the current profile
#                  (SCALE=fast default; the former macro E3/E4 were folded
#                  into E5; E3 now holds the synthetic dead-time study)
#   make full      paper-sized run (SCALE=full; hours, use a server)
#   make e1..e6    run a single experiment (e5 includes the envoy-gateway
#                  points; EG=no skips them if the EG clone is unavailable;
#                  e5 and e6 need make envtest, e6 also make gk)
#   make envtest   download the k8s control-plane binaries (etcd,
#                  kube-apiserver) that the E5 and E6 plants boot on; once
#                  per machine
#   make eg        clone upstream envoy-gateway at the pinned commit into
#                  .cache/ and overlay eg-harness/ as benchmarks/delta;
#                  once per machine (re-run after editing eg-harness/)
#   make istio     clone upstream istio at the pinned tag into .cache/ and
#                  build pilot-discovery (E5's istiod contender); once per
#                  machine
#   make gk        clone + build stock gatekeeper at the pinned tag into
#                  .cache/ (E6's baseline); once per machine
#   make render    the verification target: build dbsp, run one tiny
#                  in-process E1 point and a tiny embedded E5 probe into a
#                  scratch directory (measured results/ CSVs are never
#                  touched), then re-render every results.org to PDF+PNG
#                  from the existing CSVs
#
# Knobs (make VAR=value or environment):
#   SCALE=fast|full    sweep profile (fast = laptop-sized, full = paper-sized)
#   RENDER=yes|no      render charts after each sweep
#   EG=yes|no          run the envoy-gateway comparison points (E5)
#   ISTIO=yes|no       run the istiod comparison points (E5)
#   EG_ROOT=<path>     envoy-gateway tree with the harness overlaid
#                      (default: the clone managed by make eg)
#   EG_COMMIT=<sha>    upstream envoy-gateway commit to pin
#   ISTIO_TAG=<tag>    upstream istio tag to pin
#   ENVTEST_K8S_VERSION  k8s version for make envtest (default 1.31.0,
#                        the version the EG harness is validated on)
#
# WARNING: each experiment's run.sh starts by deleting its results/*.csv.
# Use `make render` to redo the charts without touching the data.

SCALE   ?= fast
RENDER  ?= yes

# Provisioning must never prompt for credentials. Every git the targets
# spawn - the Go toolchain's own module fetches included - runs with the
# user git config masked: an insteadOf rewrite to ssh would otherwise
# turn the anonymous https fetches into authenticated ones. Public
# anonymous https works everywhere; a fetch that would genuinely need
# auth fails loudly instead of hanging on a prompt.
export GIT_TERMINAL_PROMPT := 0
export GIT_CONFIG_GLOBAL := /dev/null
export GIT_CONFIG_SYSTEM := /dev/null
EG      ?= yes
ISTIO   ?= yes

# The envoy-gateway comparison runs against upstream EG at a pinned
# commit with eg-harness/ overlaid (the harness needs EG-internal
# packages, so it must compile inside the EG module tree).
EG_REPO   ?= https://github.com/envoyproxy/gateway
EG_COMMIT ?= 69e072f40907f1533241861bb1d881b08ea0662b
EG_CACHE  := $(CURDIR)/.cache/envoy-gateway
EG_ROOT   ?= $(EG_CACHE)

# The istiod comparison (E5) runs the stock pilot-discovery binary, built
# from the pinned upstream tag. No overlay is needed: istiod exposes its
# config surface (kubeconfig, mesh config, feature envs) on the binary.
ISTIO_REPO ?= https://github.com/istio/istio
ISTIO_TAG  ?= 1.27.9
ISTIO_CACHE := $(CURDIR)/.cache/istio
ISTIO_ROOT ?= $(ISTIO_CACHE)
ISTIO_BIN  ?= $(ISTIO_CACHE)/bin/pilot-discovery

# The Gatekeeper comparison (E6) runs the stock gatekeeper binary, built
# from the pinned upstream tag. Unlike envoy-gateway, no overlay is
# needed: the binary is the system under test, spawned by plant.
GK_REPO ?= https://github.com/open-policy-agent/gatekeeper
GK_TAG  ?= v3.23.0
GK_CACHE := $(CURDIR)/.cache/gatekeeper
GK_BIN  ?= $(GK_CACHE)/bin/gatekeeper
export SCALE RENDER EG EG_ROOT ISTIO ISTIO_ROOT ISTIO_BIN GK_ROOT=$(GK_CACHE) GK_BIN

DIRS := E1-scaling E2-robustness E3-deadtime E5-macro E6-policy-audit

# Benchmarks must not share the machine (E5 uses fixed xDS ports and one
# envtest per point), so the experiments always run sequentially, in
# numeric order, even under `make -j`.
.NOTPARALLEL:

LOCALBIN := $(CURDIR)/bin
SETUP_ENVTEST := $(LOCALBIN)/setup-envtest
ENVTEST_K8S_VERSION ?= 1.31.0

.PHONY: help build prep all full e1 e2 e3 e5 e6 envtest eg istio gk render

help:
	@awk 'NR>1 { if (!/^#/) exit; sub(/^# ?/,""); print }' Makefile

build:
	cd ../js && $(MAKE) build

# One-shot provisioning of every artifact the sweeps need, so a long run
# never pauses to download or compile.
prep: build envtest eg istio gk
	cd plant && GOWORK=off go build -o plant .
	cd "$(EG_CACHE)" && GOWORK=off go test -c -tags benchmark \
		-o benchmarks/delta/delta.test ./benchmarks/delta
	@echo "all benchmark artifacts ready; run make all / make full"

all: e1 e2 e3 e5 e6

full:
	$(MAKE) all SCALE=full

# Each experiment rebuilds dbsp first (a no-op when it is up to date).
e1: build ; ./E1-scaling/run.sh
e2: build ; ./E2-robustness/run.sh
e3: build ; ./E3-deadtime/run.sh
e5: build ; ./E5-macro/run.sh
e6: build ; ./E6-policy-audit/run.sh

# Assets land in setup-envtest's default store
# (~/.local/share/kubebuilder-envtest/k8s/<ver>-<os>-<arch>), which is
# where the E5 and E6 plants look for them.
envtest: $(SETUP_ENVTEST)
	$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION)

$(SETUP_ENVTEST):
	mkdir -p $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

eg:
	@if [ ! -d "$(EG_CACHE)/.git" ]; then \
		mkdir -p "$(EG_CACHE)" && cd "$(EG_CACHE)" && \
		git init -q && git remote add origin "$(EG_REPO)"; \
	fi
	cd "$(EG_CACHE)" && git remote set-url origin "$(EG_REPO)" && \
		git fetch -q --depth 1 origin "$(EG_COMMIT)" && \
		git checkout -q --detach FETCH_HEAD
	rm -rf "$(EG_CACHE)/benchmarks/delta"
	mkdir -p "$(EG_CACHE)/benchmarks"
	cp -a "$(CURDIR)/eg-harness" "$(EG_CACHE)/benchmarks/delta"
	@echo "envoy-gateway $(EG_COMMIT) + harness overlay ready at $(EG_CACHE)"

istio:
	@if [ ! -d "$(ISTIO_CACHE)/.git" ]; then \
		mkdir -p "$(ISTIO_CACHE)" && cd "$(ISTIO_CACHE)" && \
		git init -q && git remote add origin "$(ISTIO_REPO)"; \
	fi
	cd "$(ISTIO_CACHE)" && git remote set-url origin "$(ISTIO_REPO)" && \
		git fetch -q --depth 1 origin "refs/tags/$(ISTIO_TAG)" && \
		git checkout -q --detach FETCH_HEAD
	cd "$(ISTIO_CACHE)" && GOWORK=off go build -o bin/pilot-discovery ./pilot/cmd/pilot-discovery
	@echo "istio $(ISTIO_TAG) built at $(ISTIO_BIN)"

gk:
	@if [ ! -d "$(GK_CACHE)/.git" ]; then \
		mkdir -p "$(GK_CACHE)" && cd "$(GK_CACHE)" && \
		git init -q && git remote add origin "$(GK_REPO)"; \
	fi
	cd "$(GK_CACHE)" && git remote set-url origin "$(GK_REPO)" && \
		git fetch -q --depth 1 origin "refs/tags/$(GK_TAG)" && \
		git checkout -q --detach FETCH_HEAD
	cd "$(GK_CACHE)" && GOWORK=off go build -o bin/gatekeeper .
	@echo "gatekeeper $(GK_TAG) built at $(GK_BIN)"

# The verification target: a smoke-sized check that the drivers still run
# against the current engine, then the chart regeneration. No measured
# CSV is deleted or rewritten (every E*/run.sh does exactly that, so the
# sweeps themselves are never part of verification).
render: build
	tmp=$$(mktemp -d) && \
		(cd E1-scaling && ../../js/bin/dbsp bench.js \
			--case=join --mode=incremental --n=30 --reps=3 --out=$$tmp) && \
		(cd E5-macro && ../../js/bin/dbsp probe.js --gateways=2 --routes=2) && \
		rm -rf $$tmp
	@for d in $(DIRS); do echo "== $$d"; (cd $$d && ../render.sh results.org); done
