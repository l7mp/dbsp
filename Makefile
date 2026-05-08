GO ?= go
GOLANGCI_LINT ?= golangci-lint
SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

K8S_ENVTEST_VERSION ?= 1.30.0
K8S_LOCALBIN := connectors/kubernetes/bin
DCTRL_LOCALBIN := dcontroller/bin
K8S_LOCALBIN_ABS := $(abspath $(K8S_LOCALBIN))
DCTRL_LOCALBIN_ABS := $(abspath $(DCTRL_LOCALBIN))

.PHONY: help generate manifests build test test-fast clean test-report lint
.PHONY: build-connectors-kubernetes build-connectors-misc build-engine build-js build-dcontroller build-dcontroller-examples
.PHONY: test-connectors-kubernetes test-connectors-misc test-engine test-js test-dcontroller
.PHONY: test-fast-connectors-kubernetes test-fast-connectors-misc test-fast-engine test-fast-js test-fast-dcontroller

help:
	@printf "Root workspace targets:\n"
	@printf "  make generate     Run code generation tasks\n"
	@printf "  make manifests    Regenerate Kubernetes manifests\n"
	@printf "  make build        Build all sub-project artifacts\n"
	@printf "  make test         Run all tests (fail fast)\n"
	@printf "  make test-fast    Run fast tests (skip integration/examples)\n"
	@printf "  make test-report  Run all tests and print per-module pass/fail summary\n"
	@printf "  make lint         Run golangci-lint across workspace modules\n"
	@printf "  make clean        Remove build/test artifacts across sub-projects\n"


generate:
	@printf "==> [dcontroller] generate\n"
	@$(MAKE) -C dcontroller generate

manifests:
	@printf "==> [dcontroller] manifests\n"
	@$(MAKE) -C dcontroller manifests

build: build-connectors-kubernetes build-connectors-misc build-engine build-js build-dcontroller

build-connectors-kubernetes:
	@printf "==> [connectors/kubernetes] compile check\n"
	@$(GO) -C connectors/kubernetes test ./... -run '^$$'

build-connectors-misc:
	@printf "==> [connectors/misc] compile check\n"
	@$(GO) -C connectors/misc test ./... -run '^$$'

build-engine:
	@printf "==> [engine] compile check\n"
	@$(GO) -C engine test ./... -run '^$$'

build-js:
	@printf "==> [js] build\n"
	@$(MAKE) -C js build

build-dcontroller:
	@printf "==> [dcontroller] build\n"
	@$(MAKE) -C dcontroller build-bin
	@$(MAKE) build-dcontroller-examples

build-dcontroller-examples:
	@mkdir -p dcontroller/bin
	@pkgs="$$( $(GO) -C dcontroller list -f '{{if and (eq .Name "main") (gt (len .GoFiles) 0)}}{{.ImportPath}}{{end}}' ./examples/... )"; \
	if [ -z "$$pkgs" ]; then \
		printf "==> [dcontroller] no buildable example binaries\\n"; \
	else \
		for pkg in $$pkgs; do \
			name="$${pkg##*/}"; \
			printf "==> [dcontroller] build %s\\n" "$$name"; \
			$(GO) -C dcontroller build -trimpath -o "bin/$$name" "$$pkg"; \
		done; \
	fi

test: test-connectors-kubernetes test-connectors-misc test-engine test-js test-dcontroller

test-fast: test-fast-connectors-kubernetes test-fast-connectors-misc test-fast-engine test-fast-js test-fast-dcontroller

test-fast-connectors-kubernetes:
	@printf "==> [connectors/kubernetes] fast test\n"
	@pkgs="$$( $(GO) -C connectors/kubernetes list ./... )"; \
	keep=""; \
	for p in $$pkgs; do \
		case "$$p" in \
			*/integration|*/integration/*) ;; \
			*) keep="$$keep $$p" ;; \
		esac; \
	done; \
	$(GO) -C connectors/kubernetes test $$keep -count=1

test-fast-connectors-misc:
	@printf "==> [connectors/misc] fast test\n"
	@$(GO) -C connectors/misc test ./... -count=1

test-fast-engine:
	@printf "==> [engine] fast test\n"
	@$(GO) -C engine test ./... -count=1

test-fast-js:
	@printf "==> [js] fast test\n"
	@$(GO) -C js test ./... -count=1

test-fast-dcontroller:
	@printf "==> [dcontroller] fast test\n"
	@pkgs="$$( $(GO) -C dcontroller list ./... )"; \
	keep=""; \
	for p in $$pkgs; do \
		case "$$p" in \
			*/integration|*/integration/*|*/examples|*/examples/*) ;; \
			*) keep="$$keep $$p" ;; \
		esac; \
	done; \
	$(GO) -C dcontroller test $$keep -count=1

test-connectors-kubernetes:
	@printf "==> [connectors/kubernetes] test\n"
	@$(MAKE) -C connectors/kubernetes envtest
	@KUBEBUILDER_ASSETS="$$( $(K8S_LOCALBIN_ABS)/setup-envtest use $(K8S_ENVTEST_VERSION) --bin-dir $(K8S_LOCALBIN_ABS) -p path )" \
		$(GO) -C connectors/kubernetes test ./... -count=1

test-connectors-misc:
	@printf "==> [connectors/misc] test\n"
	@$(GO) -C connectors/misc test ./... -count=1

test-engine:
	@printf "==> [engine] test\n"
	@$(GO) -C engine test ./... -count=1

test-js:
	@printf "==> [js] test\n"
	@$(GO) -C js test ./... -count=1

test-dcontroller:
	@printf "==> [dcontroller] test\n"
	@$(MAKE) -C dcontroller envtest
	@KUBEBUILDER_ASSETS="$$( $(DCTRL_LOCALBIN_ABS)/setup-envtest use $(K8S_ENVTEST_VERSION) --bin-dir $(DCTRL_LOCALBIN_ABS) -p path )" \
		$(GO) -C dcontroller test ./... -count=1

test-report:
	@failed=0; \
	for t in test-connectors-kubernetes test-connectors-misc test-engine test-js test-dcontroller; do \
		printf "\\n---- %s ----\\n" "$$t"; \
		if $(MAKE) --no-print-directory "$$t"; then \
			printf "PASS: %s\\n" "$$t"; \
		else \
			printf "FAIL: %s\\n" "$$t"; \
			failed=1; \
		fi; \
	done; \
	if [ "$$failed" -ne 0 ]; then \
		printf "\\nOne or more module test suites failed.\\n"; \
		exit 1; \
	fi; \
	printf "\\nAll module test suites passed.\\n"

lint:
	@printf "==> [workspace] golangci-lint\n"
	@$(GOLANGCI_LINT) run ./connectors/kubernetes/... ./connectors/misc/... ./dcontroller/... ./engine/... ./js/...

clean:
	@printf "==> [connectors/kubernetes] clean\n"
	@$(MAKE) -C connectors/kubernetes clean
	@printf "==> [js] clean\n"
	@$(MAKE) -C js clean
	@printf "==> [dcontroller] clean artifacts\n"
	@rm -rf dcontroller/bin dcontroller/cover.out
