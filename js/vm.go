package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/eventloop"
	"github.com/go-logr/logr"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/relation"
	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

type VM struct {
	rt      *goja.Runtime
	loop    *eventloop.EventLoop
	runtime *dbspruntime.Runtime
	db      *relation.Database
	logger  logr.Logger
	ctx     context.Context
	cancel  context.CancelFunc

	runtimeDone      chan error
	runtimeErrCh     chan dbspruntime.Error
	closeOnce        sync.Once
	pendingCallbacks atomic.Int64
	lastActivityNS   atomic.Int64

	errMu              sync.RWMutex
	runtimeErrHandler  goja.Callable
	firstRuntimeErrOut error

	k8sMu              sync.Mutex
	k8sRuntime         *k8sruntime.Runtime
	k8sNativeAvailable bool
}

const (
	idlePollInterval   = 25 * time.Millisecond
	idleGracePeriod    = 200 * time.Millisecond
	runtimeStopTimeout = 2 * time.Second
)

func NewVM(logger logr.Logger) (*VM, error) {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	ctx, cancel := context.WithCancel(context.Background())
	v := &VM{
		loop:         eventloop.NewEventLoop(),
		runtime:      dbspruntime.NewRuntime(logger),
		db:           relation.NewDatabase("dbsp"),
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		runtimeDone:  make(chan error, 1),
		runtimeErrCh: make(chan dbspruntime.Error, dbspruntime.EventBufferSize),
	}
	v.runtime.SetErrorChannel(v.runtimeErrCh)
	v.touchActivity()
	v.logger.V(1).Info("vm created")

	v.loop.Start()
	if err := v.runOnLoopSync(func(rt *goja.Runtime) error {
		v.rt = rt
		return v.injectGlobals()
	}); err != nil {
		v.loop.Terminate()
		cancel()
		return nil, err
	}

	go func() {
		v.logger.V(1).Info("runtime manager started")
		err := v.runtime.Start(ctx)
		if err != nil && !errors.Is(ctx.Err(), context.Canceled) {
			v.logger.Error(err, "runtime failed")
			v.Close()
		}
		v.logger.V(1).Info("runtime manager stopped")
		v.runtimeDone <- err
		close(v.runtimeDone)
	}()

	go v.forwardRuntimeErrors()

	return v, nil
}

func (v *VM) Close() {
	v.closeOnce.Do(func() {
		v.logger.V(1).Info("vm shutdown requested")
		v.cancel()
		if v.loop != nil {
			v.loop.Terminate()
		}
	})
}

func (v *VM) RunFile(path string) error {
	v.touchActivity()
	v.logger.Info("running script", "path", path)
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve script path: %w", err)
	}

	src, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("read script: %w", err)
	}

	if err := v.runOnLoopSync(func(rt *goja.Runtime) error {
		_, runErr := rt.RunScript(absPath, string(src))
		return runErr
	}); err != nil {
		return fmt.Errorf("execute script %s: %w", absPath, err)
	}
	v.touchActivity()
	v.logger.V(1).Info("script loaded, entering event loop", "path", absPath)

	return v.runEventLoop()
}

func (v *VM) runEventLoop() error {
	ticker := time.NewTicker(idlePollInterval)
	defer ticker.Stop()
	v.logger.V(1).Info("event loop started")

	for {
		select {
		case <-ticker.C:
			if v.isIdle(idleGracePeriod) {
				v.logger.Info("queues drained, shutting down")
				v.Close()
			}
		case <-v.ctx.Done():
			v.logger.V(1).Info("event loop stopping")
			return v.waitRuntimeStop(runtimeStopTimeout)
		}
	}
}

func (v *VM) schedule(fn func()) {
	v.touchActivity()
	v.pendingCallbacks.Add(1)
	ok := v.loop.RunOnLoop(func(_ *goja.Runtime) {
		defer v.pendingCallbacks.Add(-1)
		defer func() {
			if r := recover(); r != nil {
				v.logger.Error(fmt.Errorf("%v", r), "js callback panic")
			}
		}()
		v.touchActivity()
		fn()
		v.touchActivity()
	})
	if !ok {
		v.pendingCallbacks.Add(-1)
		v.logger.V(1).Info("dropping scheduled callback, event loop stopped")
	}
}

func (v *VM) isIdle(idleFor time.Duration) bool {
	if v.pendingCallbacks.Load() != 0 {
		return false
	}
	last := time.Unix(0, v.lastActivityNS.Load())
	return time.Since(last) >= idleFor
}

func (v *VM) runOnLoopSync(fn func(*goja.Runtime) error) error {
	done := make(chan error, 1)
	ok := v.loop.RunOnLoop(func(rt *goja.Runtime) {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic on event loop: %v", r)
			}
		}()
		done <- fn(rt)
	})
	if !ok {
		return fmt.Errorf("event loop is not running")
	}
	return <-done
}

func (v *VM) touchActivity() {
	v.lastActivityNS.Store(time.Now().UnixNano())
}

func (v *VM) waitRuntimeStop(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = runtimeStopTimeout
	}

	select {
	case err, ok := <-v.runtimeDone:
		if rtErr := v.firstRuntimeError(); rtErr != nil {
			return rtErr
		}
		if !ok || err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("runtime failed: %w", err)
	case <-time.After(timeout):
		if rtErr := v.firstRuntimeError(); rtErr != nil {
			return rtErr
		}
		return fmt.Errorf("timed out waiting for runtime shutdown")
	}
}

func (v *VM) forwardRuntimeErrors() {
	for {
		select {
		case <-v.ctx.Done():
			return
		case rtErr := <-v.runtimeErrCh:
			h := v.runtimeErrorHandler()
			if h == nil {
				v.logger.Error(rtErr.Err, "unhandled runtime error", "origin", rtErr.Origin)
				v.recordRuntimeError(fmt.Errorf("runtime error from %s: %w", rtErr.Origin, rtErr.Err))
				continue
			}

			rtErrCopy := rtErr
			v.schedule(func() {
				payload := map[string]any{
					"origin":  rtErrCopy.Origin,
					"message": rtErrCopy.Err.Error(),
				}
				if _, err := h(goja.Undefined(), v.rt.ToValue(payload)); err != nil {
					v.recordRuntimeError(fmt.Errorf("runtime.onError callback failed: %w", err))
				}
			})
		}
	}
}

func (v *VM) runtimeErrorHandler() goja.Callable {
	v.errMu.RLock()
	defer v.errMu.RUnlock()
	return v.runtimeErrHandler
}

func (v *VM) setRuntimeErrorHandler(handler goja.Callable) {
	v.errMu.Lock()
	v.runtimeErrHandler = handler
	v.errMu.Unlock()
}

func (v *VM) recordRuntimeError(err error) {
	v.errMu.Lock()
	if v.firstRuntimeErrOut == nil {
		v.firstRuntimeErrOut = err
	}
	v.errMu.Unlock()
	v.Close()
}

func (v *VM) firstRuntimeError() error {
	v.errMu.RLock()
	defer v.errMu.RUnlock()
	return v.firstRuntimeErrOut
}

func (v *VM) toJSEntries(data zset.ZSet) ([]any, error) {
	entries := make([]any, 0, data.Size())
	var outErr error

	data.Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		payload, err := elem.MarshalJSON()
		if err != nil {
			outErr = fmt.Errorf("marshal document: %w", err)
			return false
		}

		obj := map[string]any{}
		if err := json.Unmarshal(payload, &obj); err != nil {
			outErr = fmt.Errorf("decode document json: %w", err)
			return false
		}

		entries = append(entries, []any{obj, int64(weight)})
		return true
	})

	if outErr != nil {
		return nil, outErr
	}

	return entries, nil
}

func (v *VM) fromJSEntries(value goja.Value) (zset.ZSet, error) {
	set := zset.New()
	if goja.IsUndefined(value) || goja.IsNull(value) {
		return set, nil
	}

	entries, ok := toAnySlice(value.Export())
	if !ok {
		return set, fmt.Errorf("entries must be an array")
	}

	for i, entry := range entries {
		pair, ok := toAnySlice(entry)
		if !ok || len(pair) != 2 {
			return set, fmt.Errorf("entry %d must be [document, weight]", i)
		}

		docJSON, err := json.Marshal(pair[0])
		if err != nil {
			return set, fmt.Errorf("entry %d document: %w", i, err)
		}

		doc := dbunstructured.New(map[string]any{}, nil)
		if err := doc.UnmarshalJSON(docJSON); err != nil {
			return set, fmt.Errorf("entry %d document decode: %w", i, err)
		}

		weight, err := toInt64(pair[1])
		if err != nil {
			return set, fmt.Errorf("entry %d weight: %w", i, err)
		}

		set.Insert(doc, zset.Weight(weight))
	}

	return set, nil
}

func (v *VM) injectGlobals() error {
	sqlObj := v.rt.NewObject()
	if err := sqlObj.Set("table", v.wrap(v.sqlTable)); err != nil {
		return err
	}
	if err := sqlObj.Set("compile", v.wrap(v.sqlCompile)); err != nil {
		return err
	}
	if err := v.rt.Set("sql", sqlObj); err != nil {
		return err
	}

	aggObj := v.rt.NewObject()
	if err := aggObj.Set("compile", v.wrap(v.aggregateCompile)); err != nil {
		return err
	}
	if err := v.rt.Set("aggregate", aggObj); err != nil {
		return err
	}

	if err := v.rt.Set("producer", v.wrap(v.genericProducer)); err != nil {
		return err
	}
	producerObj := v.rt.Get("producer").ToObject(v.rt)
	k8sProd := v.rt.NewObject()
	if err := k8sProd.Set("watch", v.wrap(v.k8sWatch)); err != nil {
		return err
	}
	if err := producerObj.Set("kubernetes", k8sProd); err != nil {
		return err
	}
	if err := producerObj.Set("jsonl", v.wrap(v.jsonlProducer)); err != nil {
		return err
	}

	if err := v.rt.Set("consumer", v.wrap(v.genericConsumer)); err != nil {
		return err
	}
	consumerObj := v.rt.Get("consumer").ToObject(v.rt)
	k8sCons := v.rt.NewObject()
	if err := k8sCons.Set("patcher", v.wrap(v.k8sPatcher)); err != nil {
		return err
	}
	if err := k8sCons.Set("updater", v.wrap(v.k8sUpdater)); err != nil {
		return err
	}
	if err := consumerObj.Set("kubernetes", k8sCons); err != nil {
		return err
	}
	if err := consumerObj.Set("redis", v.wrap(v.redisConsumer)); err != nil {
		return err
	}

	if err := v.rt.Set("publish", v.wrap(v.publish)); err != nil {
		return err
	}
	if err := v.rt.Set("subscribe", v.wrap(v.subscribe)); err != nil {
		return err
	}

	runtimeObj := v.rt.NewObject()
	if err := runtimeObj.Set("sql", sqlObj); err != nil {
		return err
	}
	if err := runtimeObj.Set("aggregate", aggObj); err != nil {
		return err
	}
	if err := runtimeObj.Set("producer", v.rt.Get("producer")); err != nil {
		return err
	}
	if err := runtimeObj.Set("consumer", v.rt.Get("consumer")); err != nil {
		return err
	}
	if err := runtimeObj.Set("publish", v.wrap(v.publish)); err != nil {
		return err
	}
	if err := runtimeObj.Set("subscribe", v.wrap(v.subscribe)); err != nil {
		return err
	}
	if err := runtimeObj.Set("onError", v.wrap(v.runtimeOnError)); err != nil {
		return err
	}
	if err := v.rt.Set("runtime", runtimeObj); err != nil {
		return err
	}

	return nil
}

func (v *VM) wrap(fn func(call goja.FunctionCall) (goja.Value, error)) func(call goja.FunctionCall) goja.Value {
	return func(call goja.FunctionCall) goja.Value {
		value, err := fn(call)
		if err != nil {
			panic(v.rt.NewGoError(err))
		}
		if value == nil {
			return goja.Undefined()
		}
		return value
	}
}

func toAnySlice(v any) ([]any, bool) {
	switch t := v.(type) {
	case []any:
		return t, true
	default:
		return nil, false
	}
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case uint:
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case uint64:
		return int64(n), nil
	case float32:
		f := float64(n)
		if float64(int64(f)) != f {
			return 0, fmt.Errorf("weight must be an integer")
		}
		return int64(f), nil
	case float64:
		if float64(int64(n)) != n {
			return 0, fmt.Errorf("weight must be an integer")
		}
		return int64(n), nil
	default:
		return 0, fmt.Errorf("unsupported weight type %T", v)
	}
}

func decodeOptionValue(value goja.Value, target any) error {
	if goja.IsUndefined(value) || goja.IsNull(value) {
		return nil
	}
	raw, err := json.Marshal(value.Export())
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}
