package js

import (
	"fmt"

	"github.com/dop251/goja"

	xds "github.com/l7mp/dbsp/connectors/xds"
)

type xdsServerOptions = xds.ServerSpec

type xdsConsumerOptions = xds.ConsumerSpec

type xdsProducerOptions = xds.ProducerSpec

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

	srv, err := xds.NewServerFromSpec(opts, xds.Deps{Runtime: v.runtime, Logger: v.logger})
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
	if setter {
		kind = "xds.set"
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

	// The verb decides delta vs state-of-the-world egress; the spec's
	// level flag carries the same choice on the wire.
	opts.Level = setter
	runnable, err := xds.NewConsumerFromSpec(srv, topic, opts, v.logger)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}
	if err := v.runtime.Add(runnable); err != nil {
		return nil, fmt.Errorf("%s: register consumer: %w", kind, err)
	}

	return v.boundHandle(runnable, kind, topic, opts), nil
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

	runnable, err := xds.NewProducerFromSpec(topic, opts, xds.Deps{Runtime: v.runtime, Logger: v.logger})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kind, err)
	}
	if err := v.runtime.Add(runnable); err != nil {
		return nil, fmt.Errorf("%s: register producer: %w", kind, err)
	}

	return v.boundHandle(runnable, kind, topic, opts), nil
}
