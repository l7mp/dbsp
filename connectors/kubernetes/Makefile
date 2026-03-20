LOCALBIN ?= $(shell pwd)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
CONTROLLER_TOOLS_VERSION ?= v0.16.3
ENVTEST ?= $(LOCALBIN)/setup-envtest
ENVTEST_K8S_VERSION ?= 1.30.0

.PHONY: generate manifests test clean controller-gen envtest

generate: controller-gen
	$(CONTROLLER_GEN) object paths="./..."

manifests: controller-gen
	mkdir -p config/crd/resources
	$(CONTROLLER_GEN) crd paths="./..." output:crd:artifacts:config=config/crd/resources

test: envtest
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test ./... -count=1

clean:
	rm -rf $(LOCALBIN) config/crd/resources

controller-gen: $(CONTROLLER_GEN)

envtest: $(ENVTEST)

$(CONTROLLER_GEN):
	mkdir -p $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

$(ENVTEST):
	mkdir -p $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
