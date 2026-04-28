package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
	userExitCh       chan struct{}
	internalTopicSeq atomic.Uint64

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
		userExitCh:   make(chan struct{}),
		process: processState{
			argv: []string{"dbsp"},
		},
	}
	v.runtime.SetErrorChannel(v.runtimeErrCh)
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
		return fmt.Errorf("for-await-of is not supported by this goja version; use subscribe(topic, fn) to register a callback")
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
	v.logger.V(1).Info("event loop started, waiting for exit() or signal")
	select {
	case <-v.userExitCh:
		v.logger.Info("script called exit(), shutting down")
		v.Close()
	case <-v.ctx.Done():
		v.logger.V(1).Info("event loop stopping")
	}
	return v.waitRuntimeStop(runtimeStopTimeout)
}

func (v *VM) nextInternalTopic(component, topic string) string {
	seq := v.internalTopicSeq.Add(1)
	name := fmt.Sprintf("%s-%s-%d", component, topic, seq)
	return circuit.InputTopic("js-internal", name)
}

func (v *VM) schedule(fn func()) {
	ok := v.loop.RunOnLoop(func(_ *goja.Runtime) {
		defer func() {
			if r := recover(); r != nil {
				v.logger.Error(fmt.Errorf("%v", r), "js callback panic")
			}
		}()
		fn()
	})
	if !ok {
		v.logger.V(1).Info("dropping scheduled callback, event loop stopped")
	}
}

func (v *VM) exitVM(call goja.FunctionCall) (goja.Value, error) {
	select {
	case <-v.userExitCh:
		// already signalled
	default:
		close(v.userExitCh)
	}
	return goja.Undefined(), nil
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
		if fi, statErr := os.Stat(modulePath); statErr == nil && fi.IsDir() {
			return nil, require.ModuleFileDoesNotExistError
		}
		data, err := os.ReadFile(modulePath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, require.ModuleFileDoesNotExistError
			}
			return nil, err
		}
		return data, nil
	}
	requireOptions := []require.Option{require.WithLoader(requireSourceLoader)}
	if cwd, err := os.Getwd(); err == nil {
		requireOptions = append(requireOptions, require.WithGlobalFolders(
			filepath.Join(cwd, "stdlib", "vendor"),
			filepath.Join(cwd, "js", "stdlib", "vendor"),
		))
	}
	requireRegistry := require.NewRegistry(requireOptions...)
	registerAssertModule(requireRegistry)
	registerDBSPMinimistAlias(requireRegistry)
	registerProcessModule(v, requireRegistry)
	registerFSModule(v, requireRegistry)
	registerNodeAliases(requireRegistry)
	registerTimersPromisesModule(v, requireRegistry)
	registerDBSPTestModule(v, requireRegistry)
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

	kubeObj := v.rt.NewObject()
	if err := kubeObj.Set("watch", v.wrap(v.k8sWatch)); err != nil {
		return err
	}
	if err := kubeObj.Set("list", v.wrap(v.k8sList)); err != nil {
		return err
	}
	if err := kubeObj.Set("log", v.wrap(v.k8sLog)); err != nil {
		return err
	}
	if err := kubeObj.Set("patch", v.wrap(v.k8sPatch)); err != nil {
		return err
	}
	if err := kubeObj.Set("update", v.wrap(v.k8sUpdate)); err != nil {
		return err
	}
	if err := v.rt.Set("kubernetes", kubeObj); err != nil {
		return err
	}

	fmtObj := v.rt.NewObject()
	if err := fmtObj.Set("jsonl", v.wrap(v.formatJSONL)); err != nil {
		return err
	}
	if err := fmtObj.Set("json", v.wrap(v.formatJSON)); err != nil {
		return err
	}
	if err := fmtObj.Set("yaml", v.wrap(v.formatYAML)); err != nil {
		return err
	}
	if err := fmtObj.Set("csv", v.wrap(v.formatCSV)); err != nil {
		return err
	}
	if err := fmtObj.Set("auto", v.wrap(v.formatAuto)); err != nil {
		return err
	}
	if err := v.rt.Set("format", fmtObj); err != nil {
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
	if err := v.rt.Set("exit", v.wrap(v.exitVM)); err != nil {
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
