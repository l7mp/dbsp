module github.com/l7mp/dbsp/connectors/misc

go 1.26.0

require (
	github.com/go-logr/logr v1.4.3
	github.com/l7mp/dbsp/engine v0.0.0
	github.com/onsi/ginkgo/v2 v2.28.1
	github.com/onsi/gomega v1.39.1
)

replace github.com/l7mp/dbsp/engine v0.0.0 => ../../engine

replace github.com/l7mp/dbsp/connectors/kubernetes v0.0.0 => ../kubernetes

require (
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20260302011040-a15ffb7f9dcc // indirect
	github.com/ohler55/ojg v1.28.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	golang.org/x/tools v0.43.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
