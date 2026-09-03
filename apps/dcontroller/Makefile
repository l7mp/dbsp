# Delta-controller runs as a DBSP JavaScript program inside the `dbsp` runtime,
# which is built from the js module. This Makefile covers what is specific to
# the application: the Operator CRD, the container image, and the Helm chart.

# Use only the locally installed Go toolchain (for example from Debian apt).
export GOTOOLCHAIN ?= local

# Container image to build and push.
IMG ?= docker.io/l7mp/dcontroller:latest

# Version of the kubebuilder assets envtest downloads.
ENVTEST_K8S_VERSION = 1.30.0

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# The workspace root: the container build context, and where the js module that
# builds the dbsp binary lives.
ROOT := $(abspath ../..)

.PHONY: all
all: build

.PHONY: build
build: build-bin

.PHONY: build-bin
build-bin:
	go test ./... -run '^$$'

##@ Development

.PHONY: manifests
manifests: controller-gen
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/resources

.PHONY: generate
generate: controller-gen
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint: vet

.PHONY: test
test: manifests generate fmt vet envtest
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test ./... -coverprofile cover.out

##@ Container image

# The build context is the workspace root: the image carries the dbsp binary
# built from the js module, that module's local dependencies, and the JavaScript
# stdlib the runtime loads modules from.

.PHONY: docker-build
docker-build:
	docker build -f Dockerfile -t $(IMG) $(ROOT)

.PHONY: docker-push
docker-push:
	docker push $(IMG)

.PHONY: podman-build
podman-build:
	podman build -f Dockerfile -t $(IMG) $(ROOT)

.PHONY: podman-push
podman-push:
	podman push $(IMG)

##@ Helm chart

CHART_DIR := chart/helm
CHART_REPO_DIR := chart/repo
CRD_SOURCE := config/crd/resources/dcontroller.io_operators.yaml
CHART_CRD := $(CHART_DIR)/templates/crd.yaml

# Chart and app version. A release is cut by tagging dcontroller-vX.Y.Z; any
# other commit packages a prerelease sorting below the last release, so that
# `helm repo update` on a released chart never picks up a development build.
GIT_TAG := $(shell git describe --tags --match 'dcontroller-v*' --exact-match 2>/dev/null)
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
ifneq ($(GIT_TAG),)
CHART_VERSION ?= $(patsubst dcontroller-v%,%,$(GIT_TAG))
APP_VERSION ?= $(CHART_VERSION)
else
CHART_VERSION ?= 0.0.0-dev.$(GIT_SHA)
APP_VERSION ?= latest
endif

# The published chart repository lives on this monorepo's gh-pages branch under
# the dcontroller/ prefix.
HELM_URL ?= https://l7mp.github.io/dbsp/dcontroller/

# Wrap the generated CRD in the chart's install and retention toggles. The
# annotation block controller-gen emits is the injection point.
.PHONY: chart-crd
chart-crd: manifests
	@{ \
	  echo '# Generated from $(CRD_SOURCE) by `make chart-crd`. Do not edit.'; \
	  echo '{{- if .Values.crds.install }}'; \
	  sed -e '/^---$$/d' \
	      -e 's|^  annotations:$$|  annotations:\n    {{- if .Values.crds.keep }}\n    helm.sh/resource-policy: keep\n    {{- end }}|' \
	      $(CRD_SOURCE); \
	  echo '{{- end }}'; \
	} > $(CHART_CRD)

.PHONY: chart-lint
chart-lint: helm chart-crd
	$(HELM) lint $(CHART_DIR) --set crds.install=true

# Packages the chart and (re)builds the repository index. An index already
# present in $(CHART_REPO_DIR) is merged, so earlier releases survive: the CI
# job seeds the directory from the published repository before packaging.
.PHONY: chart
chart: helm chart-crd
	mkdir -p $(CHART_REPO_DIR)
	$(HELM) package $(CHART_DIR) -d $(CHART_REPO_DIR) \
		--version $(CHART_VERSION) --app-version $(APP_VERSION)
	$(HELM) repo index $(CHART_REPO_DIR) --url $(HELM_URL) \
		$$(test -f $(CHART_REPO_DIR)/index.yaml && echo --merge $(CHART_REPO_DIR)/index.yaml)

##@ Deployment

# Both targets render the chart: it is the single description of a running
# deployment, and rendering it keeps the no-Helm path from drifting away.

.PHONY: install
install: helm chart-crd
	$(HELM) template dcontroller $(CHART_DIR) --namespace dcontroller-system \
		--show-only templates/crd.yaml | kubectl apply --server-side -f -

.PHONY: uninstall
uninstall: helm chart-crd
	$(HELM) template dcontroller $(CHART_DIR) --namespace dcontroller-system \
		--show-only templates/crd.yaml | kubectl delete --ignore-not-found -f -

.PHONY: deploy
deploy: helm chart-crd
	$(HELM) upgrade --install dcontroller $(CHART_DIR) \
		--namespace dcontroller-system --create-namespace \
		--set image.repository=$(firstword $(subst :, ,$(IMG))) \
		--set image.tag=$(lastword $(subst :, ,$(IMG)))

.PHONY: undeploy
undeploy: helm
	$(HELM) uninstall dcontroller --namespace dcontroller-system --ignore-not-found

##@ Build Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
HELM ?= $(LOCALBIN)/helm

## Tool Versions
CONTROLLER_TOOLS_VERSION ?= v0.20.1
HELM_VERSION ?= v4.1.3

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN)
$(CONTROLLER_GEN): $(LOCALBIN)
	test -s $(LOCALBIN)/controller-gen && $(LOCALBIN)/controller-gen --version | grep -q $(CONTROLLER_TOOLS_VERSION) || \
	GOTOOLCHAIN=local GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: envtest
envtest: $(ENVTEST)
$(ENVTEST): $(LOCALBIN)
	test -s $(LOCALBIN)/setup-envtest || GOTOOLCHAIN=local GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

HELM_INSTALL_SCRIPT ?= "https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4"
.PHONY: helm
helm: $(HELM)
$(HELM): $(LOCALBIN)
	test -s $(LOCALBIN)/helm && $(LOCALBIN)/helm version --short | grep -q $(HELM_VERSION) || \
	curl $(HELM_INSTALL_SCRIPT) | HELM_INSTALL_DIR=$(LOCALBIN) DESIRED_VERSION=$(HELM_VERSION) bash
