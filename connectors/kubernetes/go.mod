module github.com/l7mp/connectors

go 1.24.0

toolchain go1.24.2

require (
	github.com/go-logr/logr v1.4.3
	github.com/golang-jwt/jwt/v5 v5.2.2
	github.com/google/gnostic-models v0.7.0
	github.com/l7mp/dbsp v0.0.0
	k8s.io/api v0.34.0
	k8s.io/apiextensions-apiserver v0.34.0
	k8s.io/apimachinery v0.34.0
	k8s.io/apiserver v0.34.0
	k8s.io/client-go v0.34.0
	k8s.io/component-base v0.34.0
	k8s.io/klog/v2 v2.130.1
	k8s.io/kube-openapi v0.0.0-20250905212525-66792eed8611
	sigs.k8s.io/controller-runtime v0.22.1
	sigs.k8s.io/yaml v1.6.0
)

replace github.com/l7mp/dbsp => ../..
