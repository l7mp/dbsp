module github.com/l7mp/dbsp/connectors/misc

go 1.27.0

require (
	github.com/go-logr/logr v1.4.4
	github.com/l7mp/dbsp/engine v0.0.0
	github.com/onsi/ginkgo/v2 v2.28.1
	github.com/onsi/gomega v1.39.1
	go.yaml.in/yaml/v3 v3.0.5
)

replace github.com/l7mp/dbsp/engine v0.0.0 => ../../engine

replace github.com/l7mp/dbsp/connectors/kubernetes v0.0.0 => ../kubernetes

require (
	github.com/Masterminds/semver/v3 v3.5.0 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20261026192309-dbd0ed30aa88 // indirect
	github.com/ohler55/ojg v1.28.9 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/mod v0.42.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/tools v0.52.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
)
