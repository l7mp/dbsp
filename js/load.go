package js

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/spec"
)

// runtime.create(name[, spec]) constructs a private, named, idle DBSP
// runtime and returns its handle. With a spec (the serialized runtime
// format, engine/spec) the runtime is assembled through the connector
// registry: compilation, transforms, circuits and source/target bindings
// all happen in connector.Assemble. Nothing flows until handle.start().
// An operator is exactly such a runtime, so this is the one loader behind
// every frontend: a spec frozen from a script and an Operator CRD applied
// through dcontroller run through the same code. The handle carries the
// runtime-scoped forms of every runtime verb (publish, subscribe,
// observe, onError, the compile family); the naked globals are the same
// verbs bound to the VM's default runtime.
type runtimeInstance struct {
	vm      *VM
	name    string
	rawSpec json.RawMessage
	rt      *dbspruntime.Runtime
	reg     *dbspruntime.ConnectorRegistry
	errCh   chan dbspruntime.Error

	mu         sync.Mutex
	errHandler goja.Callable
	started    bool
	closed     bool
	cancel     context.CancelFunc
	done       chan struct{}
}

// newAmbientInstance wraps the VM's default runtime as instance zero:
// the naked globals are this instance's methods. It has no spec and no
// registry, and its lifecycle is the VM's.
func newAmbientInstance(v *VM, rt *dbspruntime.Runtime) *runtimeInstance {
	return &runtimeInstance{
		vm: v, rt: rt,
		errCh: make(chan dbspruntime.Error, dbspruntime.EventBufferSize),
		done:  make(chan struct{}),
	}
}

func (v *VM) runtimeCreate(call goja.FunctionCall) (goja.Value, error) {
	name, ok := call.Argument(0).Export().(string)
	if !ok || strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("runtime.create(name[, spec]): expected non-empty string name, got %s", describeCall(call))
	}

	v.instMu.Lock()
	if _, ok := v.instances[name]; ok {
		v.instMu.Unlock()
		return nil, fmt.Errorf("runtime.create: runtime %q already exists", name)
	}
	v.instMu.Unlock()

	rt := dbspruntime.NewRuntime(v.logger)
	rt.SetName(name)
	errCh := make(chan dbspruntime.Error, dbspruntime.EventBufferSize)
	rt.SetErrorChannel(errCh)

	reg := dbspruntime.NewConnectorRegistry()
	for _, f := range v.factories {
		if err := reg.Register(f); err != nil {
			return nil, fmt.Errorf("runtime.create: %w", err)
		}
	}

	inst := &runtimeInstance{
		vm: v, name: name, rt: rt, reg: reg, errCh: errCh,
		done: make(chan struct{}),
	}

	if arg := call.Argument(1); !goja.IsUndefined(arg) && !goja.IsNull(arg) {
		raw, err := json.Marshal(arg.Export())
		if err != nil {
			return nil, fmt.Errorf("runtime.create spec: %w", err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		var op spec.RuntimeSpec
		if err := dec.Decode(&op); err != nil {
			return nil, fmt.Errorf("runtime.create spec: %w", err)
		}
		v.compileStarted = true
		if err := rt.Assemble(&op, reg); err != nil {
			return nil, fmt.Errorf("runtime.create: %w", err)
		}
		inst.rawSpec = raw
	}

	v.instMu.Lock()
	v.instances[name] = inst
	v.instMu.Unlock()
	go inst.forwardErrors()
	return inst.jsObject()
}

// start runs the runtime: every subscription is already in place from
// construction, so the sources cannot outrun the consumers.
func (inst *runtimeInstance) start() error {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	if inst.closed {
		return fmt.Errorf("runtime %q is closed", inst.name)
	}
	if inst.started {
		return nil
	}
	inst.started = true
	ctx, cancel := context.WithCancel(inst.vm.ctx)
	inst.cancel = cancel
	go func() {
		defer close(inst.done)
		if err := inst.rt.Start(ctx); err != nil && ctx.Err() == nil {
			inst.vm.logger.Error(err, "runtime failed", "runtime", inst.name)
		}
	}()
	return nil
}

// close stops the runtime and waits, bounded, for its components to
// return, so a successor created under the same name never races this
// instance's teardown (the view unregistration included).
func (inst *runtimeInstance) close() {
	inst.mu.Lock()
	if inst.closed {
		inst.mu.Unlock()
		return
	}
	inst.closed = true
	started := inst.started
	cancel := inst.cancel
	inst.mu.Unlock()

	if started {
		cancel()
		select {
		case <-inst.done:
		case <-time.After(5 * time.Second):
			inst.vm.logger.Info("runtime close timed out", "runtime", inst.name)
		}
	}

	v := inst.vm
	v.instMu.Lock()
	delete(v.instances, inst.name)
	v.instMu.Unlock()
}

// forwardErrors drains the runtime's error channel into its onError
// handler, falling back to the VM default when none is set.
func (inst *runtimeInstance) forwardErrors() {
	v := inst.vm
	for {
		select {
		case <-v.ctx.Done():
			return
		case rtErr := <-inst.errCh:
			inst.mu.Lock()
			h := inst.errHandler
			inst.mu.Unlock()
			if h == nil {
				v.emitDefaultRuntimeError(rtErr)
				continue
			}
			rtErrCopy := rtErr
			v.schedule(func() {
				payload := map[string]any{
					"origin":  rtErrCopy.Origin,
					"message": rtErrCopy.Err.Error(),
				}
				if _, err := h(goja.Undefined(), v.rt.ToValue(payload)); err != nil {
					v.logger.Error(err, "runtime onError callback failed", "runtime", inst.name)
				}
			})
		}
	}
}

func (inst *runtimeInstance) jsObject() (*goja.Object, error) {
	v := inst.vm
	obj := v.rt.NewObject()
	_ = obj.Set("start", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if err := inst.start(); err != nil {
			return nil, err
		}
		return goja.Undefined(), nil
	}))
	_ = obj.Set("close", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		inst.close()
		return goja.Undefined(), nil
	}))
	_ = obj.Set("name", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(inst.name), nil
	}))
	_ = obj.Set("components", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(inst.rt.Components()), nil
	}))

	// The runtime-scoped verbs: the same methods the naked globals bind
	// for the default runtime.
	_ = obj.Set("publish", v.wrap(inst.publish))
	_ = obj.Set("subscribe", v.wrap(inst.subscribe))
	subObj := obj.Get("subscribe").ToObject(v.rt)
	_ = subObj.Set("once", v.wrap(inst.subscribeOnce))
	_ = obj.Set("observe", v.wrap(inst.observe))
	_ = obj.Set("onError", v.wrap(inst.onError))

	// The runtime-scoped compile family: handles install into this
	// runtime on commit.
	sqlObj, aggObj, circuitObj, err := inst.makeCompileObjects()
	if err != nil {
		return nil, err
	}
	_ = obj.Set("sql", sqlObj)
	_ = obj.Set("aggregate", aggObj)
	_ = obj.Set("circuit", circuitObj)

	// The runtime's connector registry: register built-ins by name (the
	// surface plugins will extend), list the registered ones.
	connObj := v.rt.NewObject()
	_ = connObj.Set("register", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		for i := range call.Arguments {
			name := call.Argument(i).String()
			var found *dbspruntime.ConnectorFactory
			for fi := range v.factories {
				if v.factories[fi].Name == name {
					found = &v.factories[fi]
					break
				}
			}
			if found == nil {
				return nil, fmt.Errorf("connectors.register: unknown connector %q", name)
			}
			if err := inst.reg.Register(*found); err != nil {
				return nil, fmt.Errorf("connectors.register: %w", err)
			}
		}
		return goja.Undefined(), nil
	}))
	_ = connObj.Set("list", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(inst.reg.Names()), nil
	}))
	_ = obj.Set("connectors", connObj)

	printer := v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		out := map[string]any{"name": inst.name}
		if len(inst.rawSpec) > 0 {
			var parsed any
			if err := json.Unmarshal(inst.rawSpec, &parsed); err != nil {
				return nil, err
			}
			out["spec"] = parsed
		}
		return v.rt.ToValue(out), nil
	})
	_ = obj.Set("spec", printer)
	_ = obj.Set("toJSON", printer)
	return obj, nil
}
