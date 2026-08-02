package js

import (
	"fmt"

	"github.com/dop251/goja"

	xds "github.com/l7mp/dbsp/connectors/xds"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

type xdsServerOptions struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

type xdsConsumerOptions struct {
	Type   string `json:"type"`
	Server string `json:"server"`
}

type xdsProducerOptions struct {
	Type         string         `json:"type"`
	Address      string         `json:"address"`
	Node         string         `json:"node"`
	NodeCluster  string         `json:"nodeCluster"`
	NodeMetadata map[string]any `json:"nodeMetadata"`
	Resources    []string       `json:"resources"`
	TLS          *xdsTLSOptions `json:"tls"`
	// Level selects State-of-the-World ingest: every emitted event is the
	// upstream's full resource set. The default is delta ingest.
	Level bool `json:"level"`
}

type xdsTLSOptions struct {
	Cert       string `json:"cert"`
	Key        string `json:"key"`
	CA         string `json:"ca"`
	ServerName string `json:"serverName"`
}

// newXDSNamespace builds the `xds` global: `xds.server.start({name?, address})`
// creates a named egress server; `xds.update`/`xds.set` bind egress consumers to
// a server by name; `xds.watch` ingests an upstream control plane (delta by
// default, SotW snapshots with level: true).
func (v *VM) newXDSNamespace() (*goja.Object, error) {
	obj := v.rt.NewObject()

	serverObj := v.rt.NewObject()
	if err := serverObj.Set("start", v.wrap(v.xdsServerStart)); err != nil {
		return nil, err
	}
	if err := obj.Set("server", serverObj); err != nil {
		return nil, err
	}
	if err := obj.Set("update", v.wrap(v.xdsUpdate)); err != nil {
		return nil, err
	}
	if err := obj.Set("set", v.wrap(v.xdsSet)); err != nil {
		return nil, err
	}
	if err := obj.Set("watch", v.wrap(v.xdsWatch)); err != nil {
		return nil, err
	}
	if err := obj.Set("toJSON", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(map[string]any{
			"kind": "xds",
			"apis": []string{"server.start", "update", "set", "watch"},
		}), nil
	})); err != nil {
		return nil, err
	}

	return obj, nil
}

func (v *VM) xdsServerStart(call goja.FunctionCall) (goja.Value, error) {
	var opts xdsServerOptions
	if err := decodeOptionValue(call.Argument(0), &opts); err != nil {
		return nil, fmt.Errorf("xds.server.start: %w", err)
	}

	srv, err := v.startXDSServer(opts)
	if err != nil {
		return nil, fmt.Errorf("xds.server.start: %w", err)
	}

	return v.rt.ToValue(map[string]any{
		"name":    srv.Name(),
		"address": srv.Address(),
	}), nil
}

// startXDSServer creates and registers a named egress server. The JS name (""
// for the default) keys the server registry; binding to an unstarted name is an
// error. Starting the same name twice is rejected (a name is a config domain).
func (v *VM) startXDSServer(opts xdsServerOptions) (*xds.Server, error) {
	v.xdsMu.Lock()
	defer v.xdsMu.Unlock()

	if v.xdsServers == nil {
		v.xdsServers = map[string]*xds.Server{}
	}
	if _, ok := v.xdsServers[opts.Name]; ok {
		return nil, fmt.Errorf("xds server %q already started", opts.Name)
	}

	srv, err := xds.NewServer(xds.ServerConfig{
		Name:    opts.Name,
		Address: opts.Address,
		Runtime: v.runtime,
		Logger:  v.logger,
	})
	if err != nil {
		return nil, err
	}
	if err := v.runtime.Add(srv); err != nil {
		return nil, fmt.Errorf("register server: %w", err)
	}

	v.xdsServers[opts.Name] = srv
	return srv, nil
}

func (v *VM) ensureXDSServer(name string) (*xds.Server, error) {
	v.xdsMu.Lock()
	defer v.xdsMu.Unlock()

	srv, ok := v.xdsServers[name]
	if !ok {
		if name == "" {
			return nil, fmt.Errorf("default xds server not started: call xds.server.start({address}) first")
		}
		return nil, fmt.Errorf("xds server %q not started: call xds.server.start({name, address}) first", name)
	}
	return srv, nil
}

func (v *VM) xdsUpdate(call goja.FunctionCall) (goja.Value, error) {
	return v.installXDSConsumer(call, false)
}

func (v *VM) xdsSet(call goja.FunctionCall) (goja.Value, error) {
	return v.installXDSConsumer(call, true)
}

// installXDSConsumer implements xds.update(topic, {type, server}) (delta) and
// xds.set(topic, {type, server}) (snapshot).
func (v *VM) installXDSConsumer(call goja.FunctionCall, setter bool) (goja.Value, error) {
	kind := "xds.update"
	consumerKind := "updater"
	if setter {
		kind = "xds.set"
		consumerKind = "setter"
	}

	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("%s(topic, {type, server}): expected (string topic, object opts), got %s", kind, describeCall(call))
	}
	topic, ok := call.Argument(0).Export().(string)
	if !ok || topic == "" {
		return nil, fmt.Errorf("%s(topic, {type, server}): expected non-empty string topic, got %s", kind, describeCall(call))
	}

	var opts xdsConsumerOptions
	if err := decodeOptionValue(call.Argument(1), &opts); err != nil {
		return nil, fmt.Errorf("%s options: %w", kind, err)
	}

	srv, err := v.ensureXDSServer(opts.Server)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}

	cfg := xds.UpdaterConfig{
		Name:       fmt.Sprintf("xds-consumer-%s-%s-%s", consumerKind, topic, opts.Type),
		OutputName: topic,
		Type:       opts.Type,
		Logger:     v.logger,
	}

	var runnable dbspruntime.Runnable
	if setter {
		s, err := srv.Setter(cfg)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		runnable = s
	} else {
		u, err := srv.Updater(cfg)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		runnable = u
	}
	if err := v.runtime.Add(runnable); err != nil {
		return nil, fmt.Errorf("%s: register consumer: %w", kind, err)
	}

	return v.runnableHandle(runnable), nil
}

// xdsWatch implements xds.watch(topic, {type, address, node, level}): delta
// ingest through a Watcher by default, SotW level ingest through a Lister
// with level: true.
func (v *VM) xdsWatch(call goja.FunctionCall) (goja.Value, error) {
	const kind = "xds.watch"

	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("%s(topic, {type, address, node, level}): expected (string topic, object opts), got %s", kind, describeCall(call))
	}
	topic, ok := call.Argument(0).Export().(string)
	if !ok || topic == "" {
		return nil, fmt.Errorf("%s(topic, {type, address, node, level}): expected non-empty string topic, got %s", kind, describeCall(call))
	}

	var opts xdsProducerOptions
	if err := decodeOptionValue(call.Argument(1), &opts); err != nil {
		return nil, fmt.Errorf("%s options: %w", kind, err)
	}
	if opts.Address == "" {
		return nil, fmt.Errorf("%s: empty address", kind)
	}

	producerKind := "watcher"
	if opts.Level {
		producerKind = "lister"
	}
	name := fmt.Sprintf("xds-producer-%s-%s-%s", producerKind, topic, opts.Type)
	if opts.Node != "" {
		// Several clients may feed one topic (per-gateway scoped upstreams);
		// the node id keeps the component names unique.
		name = fmt.Sprintf("%s-%s", name, opts.Node)
	}
	cfg := xds.ProducerConfig{
		Name:         name,
		InputName:    topic,
		Type:         opts.Type,
		Address:      opts.Address,
		Node:         opts.Node,
		NodeCluster:  opts.NodeCluster,
		NodeMetadata: opts.NodeMetadata,
		Resources:    opts.Resources,
		Runtime:      v.runtime,
		Logger:       v.logger,
	}
	if opts.TLS != nil {
		cfg.TLS = &xds.TLSConfig{
			CertFile:   opts.TLS.Cert,
			KeyFile:    opts.TLS.Key,
			CAFile:     opts.TLS.CA,
			ServerName: opts.TLS.ServerName,
		}
	}

	var runnable dbspruntime.Runnable
	if opts.Level {
		l, err := xds.NewLister(cfg)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		runnable = l
	} else {
		w, err := xds.NewWatcher(cfg)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
		runnable = w
	}
	if err := v.runtime.Add(runnable); err != nil {
		return nil, fmt.Errorf("%s: register producer: %w", kind, err)
	}

	return v.runnableHandle(runnable), nil
}
