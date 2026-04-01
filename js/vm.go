package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"
	gojabuffer "github.com/dop251/goja_nodejs/buffer"
	gojaconsole "github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/eventloop"
	"github.com/dop251/goja_nodejs/require"
	gojaurl "github.com/dop251/goja_nodejs/url"
	"github.com/go-logr/logr"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/relation"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

type VM struct {
	rt       *goja.Runtime
	loop     *eventloop.EventLoop
	require  *require.RequireModule
	process  processState
	runtime  *dbspruntime.Runtime
	db       *relation.Database
	logger   logr.Logger
	ctx      context.Context
	cancelVM context.CancelFunc

	runtimeDone      chan error
	runtimeErrCh     chan dbspruntime.Error
	closeOnce        sync.Once
	drainOnIdle      atomic.Bool
	internalTopicSeq atomic.Uint64
	pendingCallbacks atomic.Int64
	lastActivityNS   atomic.Int64

	errMu              sync.RWMutex
	runtimeErrHandler  goja.Callable
	firstRuntimeErrOut error

	ctxMu    sync.RWMutex
	ctxStack []cancelContext

	k8sMu              sync.Mutex
	k8sRuntime         *k8sruntime.Runtime
	k8sNativeAvailable bool
}

type processState struct {
	mu   sync.RWMutex
	argv []string
}

type cancelContext interface {
	Cancel() error
}

type cancelContextFunc func() error

func (f cancelContextFunc) Cancel() error { return f() }

const (
	idlePollInterval   = 25 * time.Millisecond
	idleGracePeriod    = 200 * time.Millisecond
	runtimeStopTimeout = 2 * time.Second
)

var (
	staticImportRe = regexp.MustCompile(`(?m)^\s*import(?:\s|\()`)
	exportRe       = regexp.MustCompile(`(?m)^\s*export\s`)
	forAwaitRe     = regexp.MustCompile(`\bfor\s+await\s*\(`)
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
		cancelVM:     cancel,
		runtimeDone:  make(chan error, 1),
		runtimeErrCh: make(chan dbspruntime.Error, dbspruntime.EventBufferSize),
		process: processState{
			argv: []string{"dbsp"},
		},
	}
	v.runtime.SetErrorChannel(v.runtimeErrCh)
	v.drainOnIdle.Store(true)
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
		v.cancelVM()
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
	if err := v.runSourceAsModule(absPath, string(src)); err != nil {
		return fmt.Errorf("execute module %s: %w", absPath, err)
	}
	v.touchActivity()
	v.logger.V(1).Info("module loaded, entering event loop", "path", absPath)

	return v.runEventLoop()
}

func (v *VM) SetProcessArgv(argv []string) error {
	if len(argv) == 0 {
		argv = []string{"dbsp"}
	}

	cpy := append([]string(nil), argv...)

	v.process.mu.Lock()
	v.process.argv = cpy
	v.process.mu.Unlock()

	if v.rt == nil {
		return nil
	}

	return v.runOnLoopSync(func(rt *goja.Runtime) error {
		proc := rt.Get("process")
		if proc == nil || goja.IsUndefined(proc) || goja.IsNull(proc) {
			return nil
		}
		return proc.ToObject(rt).Set("argv", cpy)
	})
}

func (v *VM) currentProcessArgv() []string {
	v.process.mu.RLock()
	defer v.process.mu.RUnlock()
	return append([]string(nil), v.process.argv...)
}

func (v *VM) runSourceAsModule(name, src string) error {
	if staticImportRe.MatchString(src) || strings.Contains(src, "import(") || exportRe.MatchString(src) {
		return fmt.Errorf("ES module import/export is not supported by this goja version")
	}

	if forAwaitRe.MatchString(src) {
		return fmt.Errorf("for-await-of is not supported by this goja version; use subscribe(topic).next() in a loop")
	}

	wrapped := fmt.Sprintf("(async () => {\n%s\n})();", src)
	done := make(chan error, 1)
	var settleOnce sync.Once
	settle := func(err error) {
		settleOnce.Do(func() {
			done <- err
		})
	}

	if err := v.runOnLoopSync(func(rt *goja.Runtime) error {
		value, runErr := rt.RunScript(name, wrapped)
		if runErr != nil {
			return runErr
		}

		promise, ok := value.Export().(*goja.Promise)
		if !ok {
			return fmt.Errorf("module wrapper did not return a Promise")
		}

		switch promise.State() {
		case goja.PromiseStatePending:
		case goja.PromiseStateFulfilled:
			settle(nil)
			return nil
		case goja.PromiseStateRejected:
			settle(fmt.Errorf("top-level await rejected: %v", promise.Result()))
			return nil
		}

		thenFn, ok := goja.AssertFunction(value.ToObject(rt).Get("then"))
		if !ok {
			return fmt.Errorf("module result is not thenable")
		}

		resolveHandler := rt.ToValue(func(call goja.FunctionCall) goja.Value {
			settle(nil)
			return goja.Undefined()
		})
		rejectHandler := rt.ToValue(func(call goja.FunctionCall) goja.Value {
			reason := call.Argument(0)
			settle(fmt.Errorf("top-level await rejected: %v", reason))
			return goja.Undefined()
		})

		_, err := thenFn(value, resolveHandler, rejectHandler)
		return err
	}); err != nil {
		return err
	}

	select {
	case err := <-done:
		return err
	case <-v.ctx.Done():
		return v.ctx.Err()
	}
}

func (v *VM) runEventLoop() error {
	ticker := time.NewTicker(idlePollInterval)
	defer ticker.Stop()
	v.logger.V(1).Info("event loop started")

	for {
		select {
		case <-ticker.C:
			if !v.drainOnIdle.Load() {
				continue
			}
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

func (v *VM) disableIdleDrain(reason string) {
	if v.drainOnIdle.CompareAndSwap(true, false) {
		v.logger.V(1).Info("disabling idle drain mode", "reason", reason)
	}
}

func (v *VM) nextInternalTopic(component, topic string) string {
	seq := v.internalTopicSeq.Add(1)
	name := fmt.Sprintf("%s-%s-%d", component, topic, seq)
	return circuit.InputTopic("js-internal", name)
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

		doc := dbspunstructured.New(map[string]any{}, nil)
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
	requireSourceLoader := func(modulePath string) ([]byte, error) {
		data, err := os.ReadFile(modulePath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, require.ModuleFileDoesNotExistError
			}
			return nil, err
		}
		return data, nil
	}
	requireRegistry := require.NewRegistry(require.WithLoader(requireSourceLoader))
	registerAssertModule(requireRegistry)
	registerProcessModule(v, requireRegistry)
	registerFSModule(v, requireRegistry)
	registerNodeAliases(requireRegistry)
	registerTimersPromisesModule(v, requireRegistry)
	registerDBSPTestModule(requireRegistry)
	v.require = requireRegistry.Enable(v.rt)
	gojaconsole.Enable(v.rt)
	if err := v.rt.Set("process", require.Require(v.rt, "process")); err != nil {
		return err
	}
	gojabuffer.Enable(v.rt)
	gojaurl.Enable(v.rt)
	installTextEncodingGlobals(v.rt)

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

	producerObj := v.rt.NewObject()
	k8sProd := v.rt.NewObject()
	if err := k8sProd.Set("watch", v.wrap(v.k8sWatch)); err != nil {
		return err
	}
	if err := k8sProd.Set("list", v.wrap(v.k8sList)); err != nil {
		return err
	}
	if err := producerObj.Set("kubernetes", k8sProd); err != nil {
		return err
	}
	if err := producerObj.Set("jsonl", v.wrap(v.jsonlProducer)); err != nil {
		return err
	}
	if err := v.rt.Set("producer", producerObj); err != nil {
		return err
	}

	consumerObj := v.rt.NewObject()
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
	if err := v.rt.Set("consumer", consumerObj); err != nil {
		return err
	}

	if err := v.rt.Set("publish", v.wrap(v.publish)); err != nil {
		return err
	}
	if err := v.rt.Set("subscribe", v.wrap(v.subscribeDispatch)); err != nil {
		return err
	}
	subObj := v.rt.Get("subscribe").ToObject(v.rt)
	if err := subObj.Set("once", v.wrap(v.subscribeOnce)); err != nil {
		return err
	}
	if err := v.rt.Set("cancel", v.wrap(v.cancel)); err != nil {
		return err
	}

	startTime := time.Now()
	perfObj := v.rt.NewObject()
	if err := perfObj.Set("now", func(call goja.FunctionCall) goja.Value {
		elapsed := time.Since(startTime)
		return v.rt.ToValue(float64(elapsed.Nanoseconds()) / 1e6)
	}); err != nil {
		return err
	}
	if err := v.rt.Set("performance", perfObj); err != nil {
		return err
	}

	runtimeObj := v.rt.NewObject()
	if err := runtimeObj.Set("sql", sqlObj); err != nil {
		return err
	}
	if err := runtimeObj.Set("aggregate", aggObj); err != nil {
		return err
	}
	if err := runtimeObj.Set("publish", v.wrap(v.publish)); err != nil {
		return err
	}
	if err := runtimeObj.Set("subscribe", v.rt.Get("subscribe")); err != nil {
		return err
	}
	if err := runtimeObj.Set("onError", v.wrap(v.runtimeOnError)); err != nil {
		return err
	}
	if err := runtimeObj.Set("observe", v.wrap(v.runtimeObserve)); err != nil {
		return err
	}
	if err := runtimeObj.Set("cancel", v.wrap(v.cancel)); err != nil {
		return err
	}
	if err := runtimeObj.Set("toJSON", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(map[string]any{
			"kind": "runtime",
			"apis": []string{"sql", "aggregate", "publish", "subscribe", "observe", "onError", "cancel"},
		}), nil
	})); err != nil {
		return err
	}
	if err := v.rt.Set("runtime", runtimeObj); err != nil {
		return err
	}

	return nil
}

func registerProcessModule(v *VM, reg *require.Registry) {
	reg.RegisterNativeModule("process", func(rt *goja.Runtime, module *goja.Object) {
		env := map[string]string{}
		for _, entry := range os.Environ() {
			key, value, ok := strings.Cut(entry, "=")
			if !ok {
				env[key] = ""
				continue
			}
			env[key] = value
		}
		if err := module.Get("exports").ToObject(rt).Set("env", env); err != nil {
			panic(rt.NewGoError(err))
		}
		if err := module.Get("exports").ToObject(rt).Set("argv", v.currentProcessArgv()); err != nil {
			panic(rt.NewGoError(err))
		}
	})
}

func registerAssertModule(reg *require.Registry) {
	reg.RegisterNativeModule("assert", func(rt *goja.Runtime, module *goja.Object) {
		assertObj := rt.NewObject()
		assertFailure := func(call goja.FunctionCall, fallback string) {
			msg := fallback
			if arg := call.Argument(2); !goja.IsUndefined(arg) && !goja.IsNull(arg) {
				msg = arg.String()
			}
			panic(rt.NewGoError(errors.New(msg)))
		}

		_ = assertObj.Set("ok", func(call goja.FunctionCall) goja.Value {
			if !call.Argument(0).ToBoolean() {
				msg := "assert.ok failed"
				if arg := call.Argument(1); !goja.IsUndefined(arg) && !goja.IsNull(arg) {
					msg = arg.String()
				}
				panic(rt.NewGoError(errors.New(msg)))
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("strictEqual", func(call goja.FunctionCall) goja.Value {
			if !call.Argument(0).StrictEquals(call.Argument(1)) {
				assertFailure(call, "assert.strictEqual failed")
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("notStrictEqual", func(call goja.FunctionCall) goja.Value {
			if call.Argument(0).StrictEquals(call.Argument(1)) {
				assertFailure(call, "assert.notStrictEqual failed")
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("deepStrictEqual", func(call goja.FunctionCall) goja.Value {
			a := call.Argument(0).Export()
			b := call.Argument(1).Export()
			if !reflect.DeepEqual(a, b) {
				assertFailure(call, "assert.deepStrictEqual failed")
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("fail", func(call goja.FunctionCall) goja.Value {
			msg := "assert.fail called"
			if arg := call.Argument(0); !goja.IsUndefined(arg) && !goja.IsNull(arg) {
				msg = arg.String()
			}
			panic(rt.NewGoError(errors.New(msg)))
		})

		_ = assertObj.Set("throws", func(call goja.FunctionCall) goja.Value {
			fn, ok := goja.AssertFunction(call.Argument(0))
			if !ok {
				panic(rt.NewGoError(errors.New("assert.throws expects a function")))
			}
			_, err := fn(goja.Undefined())
			if err == nil {
				panic(rt.NewGoError(errors.New("assert.throws failed")))
			}
			return goja.Undefined()
		})

		_ = assertObj.Set("rejects", func(call goja.FunctionCall) goja.Value {
			candidate := call.Argument(0)
			if fn, ok := goja.AssertFunction(candidate); ok {
				v, err := fn(goja.Undefined())
				if err != nil {
					p, resolve, _ := rt.NewPromise()
					if rerr := resolve(goja.Undefined()); rerr != nil {
						panic(rt.NewGoError(rerr))
					}
					return rt.ToValue(p)
				}
				candidate = v
			}

			obj := candidate.ToObject(rt)
			then, ok := goja.AssertFunction(obj.Get("then"))
			if !ok {
				panic(rt.NewGoError(errors.New("assert.rejects expects a Promise or async function")))
			}

			p, resolve, reject := rt.NewPromise()
			onFulfilled := rt.ToValue(func(goja.FunctionCall) goja.Value {
				if err := reject(rt.NewGoError(errors.New("assert.rejects failed"))); err != nil {
					panic(rt.NewGoError(err))
				}
				return goja.Undefined()
			})
			onRejected := rt.ToValue(func(goja.FunctionCall) goja.Value {
				if err := resolve(goja.Undefined()); err != nil {
					panic(rt.NewGoError(err))
				}
				return goja.Undefined()
			})
			if _, err := then(candidate, onFulfilled, onRejected); err != nil {
				panic(rt.NewGoError(err))
			}
			return rt.ToValue(p)
		})

		_ = module.Set("exports", assertObj)
	})
}

func registerFSModule(v *VM, reg *require.Registry) {
	reg.RegisterNativeModule("fs", func(rt *goja.Runtime, module *goja.Object) {
		fsObj := rt.NewObject()

		readFileSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			data, err := os.ReadFile(path)
			if err != nil {
				panic(rt.NewGoError(err))
			}
			enc := call.Argument(1)
			if goja.IsUndefined(enc) || goja.IsNull(enc) {
				return gojabuffer.WrapBytes(rt, data)
			}
			return gojabuffer.EncodeBytes(rt, data, enc)
		}

		toBytes := func(value goja.Value, enc goja.Value) []byte {
			return gojabuffer.DecodeBytes(rt, value, enc)
		}

		writeFileSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			data := toBytes(call.Argument(1), call.Argument(2))
			if err := os.WriteFile(path, data, 0o600); err != nil {
				panic(rt.NewGoError(err))
			}
			return goja.Undefined()
		}

		appendFileSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			data := toBytes(call.Argument(1), call.Argument(2))
			f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
			if err != nil {
				panic(rt.NewGoError(err))
			}
			defer f.Close()
			if _, err := f.Write(data); err != nil {
				panic(rt.NewGoError(err))
			}
			return goja.Undefined()
		}

		mkdirSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			recursive := false
			if opt := call.Argument(1); !goja.IsUndefined(opt) && !goja.IsNull(opt) {
				o := opt.ToObject(rt)
				recursive = o.Get("recursive").ToBoolean()
			}
			var err error
			if recursive {
				err = os.MkdirAll(path, 0o750)
			} else {
				err = os.Mkdir(path, 0o750)
			}
			if err != nil {
				panic(rt.NewGoError(err))
			}
			return goja.Undefined()
		}

		readdirSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			entries, err := os.ReadDir(path)
			if err != nil {
				panic(rt.NewGoError(err))
			}
			out := make([]any, 0, len(entries))
			for _, e := range entries {
				out = append(out, e.Name())
			}
			return rt.ToValue(out)
		}

		statSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			info, err := os.Stat(path)
			if err != nil {
				panic(rt.NewGoError(err))
			}
			o := rt.NewObject()
			_ = o.Set("size", info.Size())
			_ = o.Set("mode", uint32(info.Mode()))
			_ = o.Set("mtimeMs", float64(info.ModTime().UnixNano())/1e6)
			_ = o.Set("isFile", func(goja.FunctionCall) goja.Value { return rt.ToValue(info.Mode().IsRegular()) })
			_ = o.Set("isDirectory", func(goja.FunctionCall) goja.Value { return rt.ToValue(info.IsDir()) })
			return o
		}

		rmSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			recursive := false
			force := false
			if opt := call.Argument(1); !goja.IsUndefined(opt) && !goja.IsNull(opt) {
				o := opt.ToObject(rt)
				recursive = o.Get("recursive").ToBoolean()
				force = o.Get("force").ToBoolean()
			}
			var err error
			if recursive {
				err = os.RemoveAll(path)
			} else {
				err = os.Remove(path)
			}
			if err != nil && (!force || !errors.Is(err, fs.ErrNotExist)) {
				panic(rt.NewGoError(err))
			}
			return goja.Undefined()
		}

		existsSync := func(call goja.FunctionCall) goja.Value {
			path := call.Argument(0).String()
			_, err := os.Stat(path)
			if err == nil {
				return rt.ToValue(true)
			}
			if errors.Is(err, fs.ErrNotExist) {
				return rt.ToValue(false)
			}
			panic(rt.NewGoError(err))
		}

		_ = fsObj.Set("readFileSync", readFileSync)
		_ = fsObj.Set("writeFileSync", writeFileSync)
		_ = fsObj.Set("appendFileSync", appendFileSync)
		_ = fsObj.Set("mkdirSync", mkdirSync)
		_ = fsObj.Set("readdirSync", readdirSync)
		_ = fsObj.Set("statSync", statSync)
		_ = fsObj.Set("rmSync", rmSync)
		_ = fsObj.Set("existsSync", existsSync)

		promises := rt.NewObject()
		bindPromise := func(fn func(goja.FunctionCall) goja.Value) func(goja.FunctionCall) goja.Value {
			return func(call goja.FunctionCall) goja.Value {
				p, resolve, reject := rt.NewPromise()
				go func(args []goja.Value) {
					v.schedule(func() {
						defer func() {
							if r := recover(); r != nil {
								reason := rt.NewGoError(fmt.Errorf("%v", r))
								_ = reject(reason)
							}
						}()
						res := fn(goja.FunctionCall{Arguments: args})
						_ = resolve(res)
					})
				}(append([]goja.Value(nil), call.Arguments...))
				return rt.ToValue(p)
			}
		}
		_ = promises.Set("readFile", bindPromise(readFileSync))
		_ = promises.Set("writeFile", bindPromise(writeFileSync))
		_ = promises.Set("appendFile", bindPromise(appendFileSync))
		_ = promises.Set("mkdir", bindPromise(mkdirSync))
		_ = promises.Set("readdir", bindPromise(readdirSync))
		_ = promises.Set("stat", bindPromise(statSync))
		_ = promises.Set("rm", bindPromise(rmSync))
		_ = fsObj.Set("promises", promises)

		_ = module.Set("exports", fsObj)
	})
}

func registerTimersPromisesModule(v *VM, reg *require.Registry) {
	reg.RegisterNativeModule("timers/promises", func(rt *goja.Runtime, module *goja.Object) {
		o := rt.NewObject()
		_ = o.Set("setTimeout", func(call goja.FunctionCall) goja.Value {
			ms := call.Argument(0).ToInteger()
			if ms < 0 {
				ms = 0
			}
			value := call.Argument(1)
			p, resolve, _ := rt.NewPromise()
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
				v.schedule(func() {
					_ = resolve(value)
				})
			})
			return rt.ToValue(p)
		})
		_ = module.Set("exports", o)
	})
}

func registerDBSPTestModule(reg *require.Registry) {
	reg.RegisterNativeModule("@dbsp/test", func(rt *goja.Runtime, module *goja.Object) {
		o := rt.NewObject()
		_ = o.Set("assert", require.Require(rt, "assert"))
		_ = o.Set("sleep", func(call goja.FunctionCall) goja.Value {
			setTimeout := require.Require(rt, "timers/promises").ToObject(rt).Get("setTimeout")
			fn, ok := goja.AssertFunction(setTimeout)
			if !ok {
				panic(rt.NewGoError(errors.New("timers/promises.setTimeout is not a function")))
			}
			value, err := fn(goja.Undefined(), call.Argument(0))
			if err != nil {
				panic(rt.NewGoError(err))
			}
			return value
		})
		_ = module.Set("exports", o)
	})
}

func registerNodeAliases(reg *require.Registry) {
	aliases := map[string]string{
		"node:assert":          "assert",
		"node:buffer":          "buffer",
		"node:console":         "console",
		"node:process":         "process",
		"node:url":             "url",
		"node:util":            "util",
		"node:fs":              "fs",
		"node:fs/promises":     "fs",
		"node:timers/promises": "timers/promises",
	}
	for alias, target := range aliases {
		aliasCopy := alias
		targetCopy := target
		reg.RegisterNativeModule(aliasCopy, func(rt *goja.Runtime, module *goja.Object) {
			if targetCopy == "fs" && aliasCopy == "node:fs/promises" {
				fsModule := require.Require(rt, "fs").ToObject(rt)
				_ = module.Set("exports", fsModule.Get("promises"))
				return
			}
			_ = module.Set("exports", require.Require(rt, targetCopy))
		})
	}
}

func installTextEncodingGlobals(rt *goja.Runtime) {
	_, err := rt.RunString(`
if (typeof TextEncoder === "undefined") {
  globalThis.TextEncoder = class TextEncoder {
    encode(input = "") { return Buffer.from(String(input), "utf8"); }
  };
}
if (typeof TextDecoder === "undefined") {
  globalThis.TextDecoder = class TextDecoder {
    constructor(label = "utf8") { this.label = String(label || "utf8"); }
    decode(input) {
      if (input == null) return "";
      if (typeof input === "string") return input;
      return Buffer.from(input).toString(this.label);
    }
  };
}
`)
	if err != nil {
		panic(rt.NewGoError(err))
	}
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

func (v *VM) withCancelContext(ctx cancelContext, run func()) {
	v.ctxMu.Lock()
	v.ctxStack = append(v.ctxStack, ctx)
	v.ctxMu.Unlock()

	defer func() {
		v.ctxMu.Lock()
		v.ctxStack = v.ctxStack[:len(v.ctxStack)-1]
		v.ctxMu.Unlock()
	}()

	run()
}

func (v *VM) currentCancelContext() cancelContext {
	v.ctxMu.RLock()
	defer v.ctxMu.RUnlock()

	if len(v.ctxStack) == 0 {
		return cancelContextFunc(func() error {
			v.cancelVM()
			return nil
		})
	}

	return v.ctxStack[len(v.ctxStack)-1]
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
