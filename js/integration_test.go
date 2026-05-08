package js

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/engine/datamodel"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type collectingConsumer struct {
	*dbspruntime.BaseConsumer

	mu     sync.Mutex
	events []dbspruntime.Event
}

func newCollectingConsumer(name string, rt *dbspruntime.Runtime, topic string) (*collectingConsumer, error) {
	base, err := dbspruntime.NewBaseConsumer(dbspruntime.BaseConsumerConfig{
		Name:          name,
		Subscriber:    rt.NewSubscriber(),
		ErrorReporter: rt,
		Logger:        logr.Discard(),
		Topics:        []string{topic},
	})
	if err != nil {
		return nil, err
	}

	return &collectingConsumer{BaseConsumer: base, events: []dbspruntime.Event{}}, nil
}

func (c *collectingConsumer) Start(ctx context.Context) error {
	return c.Run(ctx, c)
}

func (c *collectingConsumer) Consume(ctx context.Context, e dbspruntime.Event) error {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
	return nil
}

func (c *collectingConsumer) Snapshot() []dbspruntime.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]dbspruntime.Event, len(c.events))
	copy(out, c.events)
	return out
}

type runtimeErrorEmitter struct {
	*dbspruntime.BaseProducer
	err error
}

func newRuntimeErrorEmitter(name string, rt *dbspruntime.Runtime, err error) (*runtimeErrorEmitter, error) {
	base, e := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          name,
		Publisher:     rt.NewPublisher(),
		ErrorReporter: rt,
		Logger:        logr.Discard(),
	})
	if e != nil {
		return nil, e
	}

	return &runtimeErrorEmitter{BaseProducer: base, err: err}, nil
}

func (e *runtimeErrorEmitter) Start(ctx context.Context) error {
	e.HandleError(e.err)
	<-ctx.Done()
	return nil
}

func runScript(vm *VM, script string) error {
	return vm.runOnLoopSync(func(rt *goja.Runtime) error {
		_, err := rt.RunString(script)
		return err
	})
}

func runScriptAsModule(vm *VM, script string) error {
	return vm.runSourceAsModule("<inline-module>", script)
}

func zsetRowsByField(ev dbspruntime.Event, field string) []string {
	rows := make([]string, 0, ev.Data.Size())
	ev.Data.Iter(func(doc datamodel.Document, weight zset.Weight) bool {
		v, err := doc.GetField(field)
		if err == nil {
			rows = append(rows, fmt.Sprintf("%d:%v", weight, v))
		}
		return true
	})
	sort.Strings(rows)
	return rows
}

//nolint:unused
func zsetRowsJSON(ev dbspruntime.Event) []string {
	rows := make([]string, 0, ev.Data.Size())
	ev.Data.Iter(func(doc datamodel.Document, weight zset.Weight) bool {
		b, err := doc.MarshalJSON()
		if err == nil {
			rows = append(rows, fmt.Sprintf("%d:%s", weight, string(b)))
		}
		return true
	})
	sort.Strings(rows)
	return rows
}

var _ = Describe("VM integration", func() {
	type scriptCase struct {
		name     string
		script   string
		topic    string
		expected []string
	}

	tests := []scriptCase{
		{
			name:  "publish shorthand",
			topic: "test-out",
			script: `
publish("test-out", [[{id: 1, name: "alpha"}, 1], [{id: 2, name: "beta"}, 1]]);
`,
			expected: []string{"1:1", "1:2"},
		},
		{
			name:  "runtime.publish alias",
			topic: "test-out",
			script: `
runtime.publish("test-out", [[{id: 42}, 3]]);
`,
			expected: []string{"3:42"},
		},
		{
			name:  "subscribe shorthand receives and republishes",
			topic: "test-out",
			script: `
subscribe("input-topic", (entries) => {
  runtime.publish("test-out", entries);
});
publish("input-topic", [[{id: 7}, 1]]);
`,
			expected: []string{"1:7"},
		},
	}

	for _, tc := range tests {
		tc := tc
		It(tc.name, func() {
			vm, err := NewVM(logr.Discard())
			Expect(err).NotTo(HaveOccurred())
			defer vm.Close()

			collector, err := newCollectingConsumer("collector", vm.runtime, tc.topic)
			Expect(err).NotTo(HaveOccurred())
			Expect(vm.runtime.Add(collector)).To(Succeed())

			Expect(runScript(vm, tc.script)).To(Succeed())

			var first dbspruntime.Event
			Eventually(func() bool {
				events := collector.Snapshot()
				if len(events) == 0 {
					return false
				}
				first = events[0]
				return true
			}, time.Second, 10*time.Millisecond).Should(BeTrue())

			Expect(first.Name).To(Equal(tc.topic))
			Expect(zsetRowsByField(first, "id")).To(Equal(tc.expected))
		})
	}

	It("supports top-level await in module scripts", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("tla-collector", vm.runtime, "tla-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = runScript(vm, `publish("tla-in", [[{id: 42}, 1]]);`)
		}()

		script := `
const result = await subscribe.once("tla-in");
publish("tla-out", result);
`
		Expect(runScriptAsModule(vm, script)).To(Succeed())
		Eventually(func() int {
			return len(collector.Snapshot())
		}, 2*time.Second, 10*time.Millisecond).Should(Equal(1))
	})

	It("subscribe.once resolves with first event", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("once-collector", vm.runtime, "once-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const first = await subscribe.once("once-trigger");
publish("once-out", first);
`
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = runScript(vm, `publish("once-trigger", [[{id: 99}, 1]]);`)
		}()
		Expect(runScriptAsModule(vm, script)).To(Succeed())
		Eventually(func() int {
			return len(collector.Snapshot())
		}, 2*time.Second, 10*time.Millisecond).Should(Equal(1))
	})

	It("subscribe.then receives events from topic", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("iter-collector", vm.runtime, "iter-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
let count = 0;
subscribe("iter-in", (entries) => {
  count += entries.length;
  if (count >= 3) {
    publish("iter-out", [[{total: count}, 1]]);
  }
});
`
		go func() {
			time.Sleep(50 * time.Millisecond)
			for i := 0; i < 5; i++ {
				_ = runScript(vm, fmt.Sprintf(`publish("iter-in", [[{id: %d}, 1]]);`, i))
				time.Sleep(10 * time.Millisecond)
			}
		}()

		Expect(runScript(vm, script)).To(Succeed())
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			rows := zsetRowsByField(events[0], "total")
			return len(rows) > 0 && rows[0] == "1:3"
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
	})

	It("rejects import statements with a clear module error", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScriptAsModule(vm, `import "./lib.js";`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ES module import/export is not supported"))
	})

	It("supports commonjs require for local scripts", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		tmpDir, err := os.MkdirTemp("", "dbsp-js-require-")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(os.RemoveAll(tmpDir)).To(Succeed())
		})

		libPath := filepath.Join(tmpDir, "lib.js")
		Expect(os.WriteFile(libPath, []byte("module.exports = { value: 42, add: (a, b) => a + b };\n"), 0o600)).To(Succeed())

		err = vm.runOnLoopSync(func(rt *goja.Runtime) error {
			modV, callErr := rt.RunString(fmt.Sprintf(`require(%q)`, libPath))
			if callErr != nil {
				return callErr
			}
			modObj := modV.ToObject(rt)
			if got := modObj.Get("value"); !got.StrictEquals(rt.ToValue(42)) {
				return fmt.Errorf("unexpected module value: %v", got)
			}
			addFn, ok := goja.AssertFunction(modObj.Get("add"))
			if !ok {
				return fmt.Errorf("module add export is not a function")
			}
			res, callErr := addFn(modObj, rt.ToValue(2), rt.ToValue(3))
			if callErr != nil {
				return callErr
			}
			if !res.StrictEquals(rt.ToValue(5)) {
				return fmt.Errorf("unexpected add result: %v", res)
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("provides require('assert') helpers", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScriptAsModule(vm, `
const assert = require("assert");
assert.ok(true);
assert.strictEqual(2 + 2, 4);
assert.notStrictEqual(2 + 2, 5);
assert.deepStrictEqual({ a: 1, b: [2, 3] }, { a: 1, b: [2, 3] });
assert.throws(() => { throw new Error("boom"); });
await assert.rejects(Promise.reject(new Error("boom")));
`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("supports node:-prefixed core module aliases", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScriptAsModule(vm, `
const assertA = require("assert");
const assertB = require("node:assert");
assertA.strictEqual(assertA, assertB);

const fsA = require("fs");
const fsB = require("node:fs");
assertA.strictEqual(fsA, fsB);

const fsp = require("node:fs/promises");
assertA.strictEqual(typeof fsp.readFile, "function");

const tp = require("node:timers/promises");
assertA.strictEqual(typeof tp.setTimeout, "function");
`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("supports fs sync and promises APIs", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		tmpDir, err := os.MkdirTemp("", "dbsp-js-fs-")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(os.RemoveAll(tmpDir)).To(Succeed())
		})

		script := fmt.Sprintf(`
const assert = require("assert");
const fs = require("fs");
const fsp = require("node:fs/promises");

const dir = %q;
const p = dir + "/out.txt";

fs.writeFileSync(p, "alpha", "utf8");
assert.strictEqual(fs.readFileSync(p, "utf8"), "alpha");
fs.appendFileSync(p, "-beta", "utf8");
assert.strictEqual(fs.readFileSync(p, "utf8"), "alpha-beta");

const entries = fs.readdirSync(dir);
assert.ok(entries.includes("out.txt"));

const st = fs.statSync(p);
assert.ok(st.isFile());
assert.strictEqual(fs.existsSync(p), true);

await fsp.writeFile(p, "gamma", "utf8");
assert.strictEqual(await fsp.readFile(p, "utf8"), "gamma");
await fsp.rm(p);
assert.strictEqual(fs.existsSync(p), false);
`, tmpDir)

		err = runScriptAsModule(vm, script)
		Expect(err).NotTo(HaveOccurred())
	})

	It("exposes timers/promises setTimeout", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScriptAsModule(vm, `
const assert = require("assert");
const timers = require("timers/promises");
const started = performance.now();
const v = await timers.setTimeout(5, 42);
assert.strictEqual(v, 42);
assert.ok(performance.now() >= started);
`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("exposes TextEncoder and TextDecoder globals", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScriptAsModule(vm, `
const assert = require("assert");
const enc = new TextEncoder();
const bytes = enc.encode("hello");
assert.strictEqual(Buffer.from(bytes).toString("utf8"), "hello");
const dec = new TextDecoder("utf8");
assert.strictEqual(dec.decode(bytes), "hello");
`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("exposes @dbsp/test helpers", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScriptAsModule(vm, `
const t = require("@dbsp/test");
t.assert.ok(true);
await t.sleep(2);
t.assert.strictEqual(1 + 1, 2);
`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("exposes minimist parse helper", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScriptAsModule(vm, `
const assert = require("assert");
const minimist = require("minimist");
const parsed = minimist(["test", "gwclass", "--suite=gwclass", "--verbose", "-vf"]);
assert.strictEqual(parsed._[0], "test");
assert.strictEqual(parsed._[1], "gwclass");
assert.strictEqual(parsed.suite, "gwclass");
assert.strictEqual(parsed.verbose, true);
assert.strictEqual(parsed.v, true);
assert.strictEqual(parsed.f, true);
`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("exposes goja_nodejs process env", func() {
		Expect(os.Setenv("DBSP_JS_TEST_ENV", "present")).To(Succeed())
		DeferCleanup(func() {
			Expect(os.Unsetenv("DBSP_JS_TEST_ENV")).To(Succeed())
		})

		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = vm.runOnLoopSync(func(rt *goja.Runtime) error {
			process := rt.Get("process")
			if process == nil || goja.IsUndefined(process) || goja.IsNull(process) {
				return fmt.Errorf("process is not defined")
			}
			env := process.ToObject(rt).Get("env")
			if env == nil || goja.IsUndefined(env) || goja.IsNull(env) {
				return fmt.Errorf("process.env is not defined")
			}
			got := env.ToObject(rt).Get("DBSP_JS_TEST_ENV")
			if got == nil {
				return fmt.Errorf("process.env.DBSP_JS_TEST_ENV is missing")
			}
			if !got.StrictEquals(rt.ToValue("present")) {
				return fmt.Errorf("unexpected process.env value: %v", got)
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("allows overriding process.argv for script arguments", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = vm.SetProcessArgv([]string{"dbsp", "examples/gwapi/index.js", "test", "gwclass"})
		Expect(err).NotTo(HaveOccurred())

		err = vm.runOnLoopSync(func(rt *goja.Runtime) error {
			process := rt.Get("process")
			if process == nil || goja.IsUndefined(process) || goja.IsNull(process) {
				return fmt.Errorf("process is not defined")
			}
			argv := process.ToObject(rt).Get("argv")
			if argv == nil || goja.IsUndefined(argv) || goja.IsNull(argv) {
				return fmt.Errorf("process.argv is not defined")
			}
			arr := argv.ToObject(rt)
			if got := arr.Get("0"); !got.StrictEquals(rt.ToValue("dbsp")) {
				return fmt.Errorf("unexpected argv[0]: %v", got)
			}
			if got := arr.Get("1"); !got.StrictEquals(rt.ToValue("examples/gwapi/index.js")) {
				return fmt.Errorf("unexpected argv[1]: %v", got)
			}
			if got := arr.Get("2"); !got.StrictEquals(rt.ToValue("test")) {
				return fmt.Errorf("unexpected argv[2]: %v", got)
			}
			if got := arr.Get("3"); !got.StrictEquals(rt.ToValue("gwclass")) {
				return fmt.Errorf("unexpected argv[3]: %v", got)
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("exposes goja_nodejs Buffer and URL globals", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = vm.runOnLoopSync(func(rt *goja.Runtime) error {
			bufferCtor := rt.Get("Buffer")
			if goja.IsUndefined(bufferCtor) || goja.IsNull(bufferCtor) {
				return fmt.Errorf("Buffer is not defined")
			}

			bufferObj := bufferCtor.ToObject(rt)
			fromFn, ok := goja.AssertFunction(bufferObj.Get("from"))
			if !ok {
				return fmt.Errorf("Buffer.from is not a function")
			}
			bufV, callErr := fromFn(bufferObj, rt.ToValue("hello"))
			if callErr != nil {
				return callErr
			}
			toStringFn, ok := goja.AssertFunction(bufV.ToObject(rt).Get("toString"))
			if !ok {
				return fmt.Errorf("Buffer#toString is not a function")
			}
			strV, callErr := toStringFn(bufV, rt.ToValue("utf8"))
			if callErr != nil {
				return callErr
			}
			if !strV.StrictEquals(rt.ToValue("hello")) {
				return fmt.Errorf("unexpected Buffer decode: %v", strV)
			}

			urlCtor := rt.Get("URL")
			if goja.IsUndefined(urlCtor) || goja.IsNull(urlCtor) {
				return fmt.Errorf("URL is not defined")
			}
			urlObj, callErr := rt.New(urlCtor.ToObject(rt), rt.ToValue("https://example.com/path?a=1"))
			if callErr != nil {
				return callErr
			}
			if href := urlObj.Get("href"); !href.StrictEquals(rt.ToValue("https://example.com/path?a=1")) {
				return fmt.Errorf("unexpected URL href: %v", href)
			}

			return nil
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects for-await-of with fallback guidance", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScriptAsModule(vm, `for await (const e of subscribe("x", () => {})) { break; }`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("for-await-of is not supported"))
		Expect(err.Error()).To(ContainSubstring("subscribe(topic, fn)"))
	})

	It("subscribe requires two arguments", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `subscribe("topic");`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("subscribe(topic, fn) requires topic and callback"))
	})

	It("kubernetes.patch is available", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `typeof kubernetes.patch`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("transforms forwarded events with registerProducerCallback", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("transform-collector", vm.runtime, "transform-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		var fn goja.Callable
		err = vm.runOnLoopSync(func(rt *goja.Runtime) error {
			value, runErr := rt.RunString(`(entries) => [[{id: entries[0][0].id + 1}, entries[0][1]]]`)
			if runErr != nil {
				return runErr
			}
			cb, ok := goja.AssertFunction(value)
			if !ok {
				return fmt.Errorf("producer callback is not a function")
			}
			fn = cb
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		vm.registerProducerCallback("transform-in", "transform-out", "test-transform", fn)
		Expect(runScript(vm, `publish("transform-in", [[{id: 1}, 1]]);`)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(zsetRowsByField(first, "id")).To(Equal([]string{"1:2"}))
	})

	It("publishes empty Z-set when producer callback returns undefined", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("producer-undef-collector", vm.runtime, "producer-undef-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		var fn goja.Callable
		err = vm.runOnLoopSync(func(rt *goja.Runtime) error {
			value, runErr := rt.RunString(`(_entries) => {}`)
			if runErr != nil {
				return runErr
			}
			cb, ok := goja.AssertFunction(value)
			if !ok {
				return fmt.Errorf("producer callback is not a function")
			}
			fn = cb
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		vm.registerProducerCallback("producer-undef-in", "producer-undef-out", "test-producer", fn)
		Expect(runScript(vm, `publish("producer-undef-in", [[{id: 7}, 1]]);`)).To(Succeed())

		// Undefined return → empty Z-set is published, so the collector receives one event.
		Eventually(func() int {
			return len(collector.Snapshot())
		}, 2*time.Second, 10*time.Millisecond).Should(Equal(1))
		Expect(collector.Snapshot()[0].Data.Size()).To(Equal(0))
	})

	It("publishes empty Z-set when producer callback returns null", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("producer-null-collector", vm.runtime, "producer-null-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		var fn goja.Callable
		err = vm.runOnLoopSync(func(rt *goja.Runtime) error {
			value, runErr := rt.RunString(`(_entries) => null`)
			if runErr != nil {
				return runErr
			}
			cb, ok := goja.AssertFunction(value)
			if !ok {
				return fmt.Errorf("producer callback is not a function")
			}
			fn = cb
			return nil
		})
		Expect(err).NotTo(HaveOccurred())

		vm.registerProducerCallback("producer-null-in", "producer-null-out", "test-producer", fn)
		Expect(runScript(vm, `publish("producer-null-in", [[{id: 9}, 1]]);`)).To(Succeed())

		// Null return → empty Z-set is published, so the collector receives one event.
		Eventually(func() int {
			return len(collector.Snapshot())
		}, 2*time.Second, 10*time.Millisecond).Should(Equal(1))
		Expect(collector.Snapshot()[0].Data.Size()).To(Equal(0))
	})

	It("validates kubernetes.watch callback type before startup", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `
kubernetes.watch("services", {
  gvk: "v1/Service",
  namespace: "default",
}, 42);
`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("kubernetes.watch callback must be a function"))
	})

	It("validates kubernetes.list callback type before startup", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		err = runScript(vm, `
kubernetes.list("services", {
  gvk: "v1/Service",
  namespace: "default",
}, 42);
`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("kubernetes.list callback must be a function"))
	})

	It("invokes runtime.onError callback for async runtime errors", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("err-collector", vm.runtime, "runtime-errors")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
runtime.onError((e) => {
  runtime.publish("runtime-errors", [[{origin: e.origin, message: e.message}, 1]]);
});
`
		Expect(runScript(vm, script)).To(Succeed())

		sentinel := errors.New("boom from emitter")
		emitter, err := newRuntimeErrorEmitter("emitter-1", vm.runtime, sentinel)
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(emitter)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		entries := first.Data.Entries()
		Expect(entries).NotTo(BeEmpty())
		origin, err := entries[0].Document.GetField("origin")
		Expect(err).NotTo(HaveOccurred())
		message, err := entries[0].Document.GetField("message")
		Expect(err).NotTo(HaveOccurred())

		Expect(origin).To(Equal("emitter-1"))
		Expect(fmt.Sprint(message)).To(ContainSubstring(sentinel.Error()))
	})

	It("supports cancel() inside consumer callback", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("cancel-collector", vm.runtime, "cancel-events")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
let called = 0;
subscribe("cancel-in", (entries) => {
  called += 1;
  runtime.publish("cancel-events", [[{count: called}, 1]]);
  cancel();
});

publish("cancel-in", [[{id: 1}, 1]]);
publish("cancel-in", [[{id: 2}, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() int {
			return len(collector.Snapshot())
		}, 2*time.Second, 10*time.Millisecond).Should(Equal(1))
	})

	It("cancel() outside callback context stops the script", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		Expect(runScript(vm, `cancel();`)).To(Succeed())
	})

	It("invokes circuit.observe callback with execution payload", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("observe-collector", vm.runtime, "observe-events")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		outCollector, err := newCollectingConsumer("observe-out-collector", vm.runtime, "obs-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(outCollector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "obs-in",
  outputs: ["obs-out"]
});

c.observe((e) => {
  runtime.publish("observe-events", [[{
    node: e.node.id,
    kind: e.node.kind,
    position: e.position,
    scheduleLen: e.schedule.length
  }, 1]]);
});

c.validate();
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			if err := runScript(vm, `publish("obs-in", [[{id: 101}, 1]]);`); err != nil {
				return false
			}
			events := outCollector.Snapshot()
			return len(events) > 0
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		var first dbspruntime.Event
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		entries := first.Data.Entries()
		Expect(entries).NotTo(BeEmpty())
		node, err := entries[0].Document.GetField("node")
		Expect(err).NotTo(HaveOccurred())
		kind, err := entries[0].Document.GetField("kind")
		Expect(err).NotTo(HaveOccurred())
		scheduleLen, err := entries[0].Document.GetField("scheduleLen")
		Expect(err).NotTo(HaveOccurred())

		Expect(fmt.Sprint(node)).NotTo(BeEmpty())
		Expect(fmt.Sprint(kind)).NotTo(BeEmpty())
		Expect(fmt.Sprint(scheduleLen)).NotTo(Equal("0"))
	})

	It("invokes runtime.observe callback for a named circuit", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("runtime-observe-collector", vm.runtime, "runtime-observe-events")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		outCollector, err := newCollectingConsumer("runtime-observe-out-collector", vm.runtime, "obs-runtime-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(outCollector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "obs-runtime-in",
  outputs: ["obs-runtime-out"]
});

c.validate();

runtime.observe("aggregation", (e) => {
  runtime.publish("runtime-observe-events", [[{
    node: e.node.id,
    position: e.position
  }, 1]]);
});
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			if err := runScript(vm, `publish("obs-runtime-in", [[{id: 202}, 1]]);`); err != nil {
				return false
			}
			events := outCollector.Snapshot()
			return len(events) > 0
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		var first dbspruntime.Event
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		entries := first.Data.Entries()
		Expect(entries).NotTo(BeEmpty())
		node, err := entries[0].Document.GetField("node")
		Expect(err).NotTo(HaveOccurred())
		Expect(fmt.Sprint(node)).NotTo(BeEmpty())
	})

	It("respects aggregate.compile name option for runtime.observe", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("runtime-observe-custom-collector", vm.runtime, "runtime-observe-custom-events")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "obs-custom-in",
  outputs: ["obs-custom-out"],
  name: "custom-aggregation"
});

runtime.observe("custom-aggregation", (e) => {
  runtime.publish("runtime-observe-custom-events", [[{node: e.node.id}, 1]]);
  cancel();
});

publish("obs-custom-in", [[{id: 303}, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			entries := events[0].Data.Entries()
			if len(entries) == 0 {
				return false
			}
			node, err := entries[0].Document.GetField("node")
			if err != nil {
				return false
			}
			return fmt.Sprint(node) != ""
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
	})

	It("prints JSON for runtime objects and documents via console.log", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("console-json-collector", vm.runtime, "console-json")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "console-in",
  outputs: ["console-out"]
});

c.validate();

console.log(c);
console.log(runtime);

subscribe("console-out", (entries) => {
  for (const [doc, weight] of entries) {
    console.log(doc);
    runtime.publish("console-json", [[doc, weight]]);
  }
  cancel();
});

publish("console-in", [[{id: 77, name: "json"}, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		rows := zsetRowsByField(first, "id")
		Expect(rows).To(Equal([]string{"1:77"}))
	})

	It("exposes console.error without throwing", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
console.error("test-error", { code: 42 });
`
		Expect(runScript(vm, script)).To(Succeed())
	})

	It("runs without kubeconfig until kubernetes plumbing is requested", func() {
		oldKubeconfig, hadKubeconfig := os.LookupEnv("KUBECONFIG")
		Expect(os.Setenv("KUBECONFIG", "/nonexistent/kubeconfig")).To(Succeed())
		DeferCleanup(func() {
			if hadKubeconfig {
				Expect(os.Setenv("KUBECONFIG", oldKubeconfig)).To(Succeed())
				return
			}
			Expect(os.Unsetenv("KUBECONFIG")).To(Succeed())
		})

		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		Expect(runScript(vm, `runtime.publish("plain", [[{id: 1}, 1]]);`)).To(Succeed())
		Expect(vm.k8sRuntime).To(BeNil())
		Expect(runScript(vm, `kubernetes.runtime.start();`)).To(Succeed())

		_, err = vm.parseGVK("v1/Pod")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("only view resources can be used"))

		viewGVK := schema.GroupVersionKind{Group: "demo.view.dcontroller.io", Version: "v1alpha1", Kind: "Widget"}
		got, err := vm.parseGVK("demo.view.dcontroller.io/v1alpha1/Widget")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(viewGVK))
	})

	It("compiles aggregate pipelines with string input/output names", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("agg-collector", vm.runtime, "agg-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "agg-in",
  outputs: ["agg-out"]
});
c.validate();
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			_ = runScript(vm, `publish("agg-in", [[{id: 11}, 1]]);`)
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(first.Name).To(Equal("agg-out"))
		Expect(zsetRowsByField(first, "id")).To(Equal([]string{"1:11"}))
	})

	It("compiles aggregate pipelines with multiple outputs", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collectorA, err := newCollectingConsumer("agg-multi-out-a", vm.runtime, "agg-multi-out-a")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collectorA)).To(Succeed())

		collectorB, err := newCollectingConsumer("agg-multi-out-b", vm.runtime, "agg-multi-out-b")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collectorB)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "agg-multi-in",
  outputs: ["agg-multi-out-a", "agg-multi-out-b"]
});
c.validate();
publish("agg-multi-in", [[{id: 909}, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			events := collectorA.Snapshot()
			if len(events) == 0 {
				return false
			}
			return len(zsetRowsByField(events[0], "id")) > 0
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Eventually(func() bool {
			events := collectorB.Snapshot()
			if len(events) == 0 {
				return false
			}
			return len(zsetRowsByField(events[0], "id")) > 0
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		eventsA := collectorA.Snapshot()
		eventsB := collectorB.Snapshot()
		Expect(zsetRowsByField(eventsA[0], "id")).To(Equal([]string{"1:909"}))
		Expect(zsetRowsByField(eventsB[0], "id")).To(Equal([]string{"1:909"}))
	})

	It("auto-registers aggregate circuits without explicit validate", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("agg-auto-collector", vm.runtime, "agg-auto-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "agg-auto-in",
  outputs: ["agg-auto-out"]
});
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			_ = runScript(vm, `publish("agg-auto-in", [[{id: 111}, 1]]);`)
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(first.Name).To(Equal("agg-auto-out"))
		Expect(zsetRowsByField(first, "id")).To(Equal([]string{"1:111"}))
	})

	It("transform(Incrementalizer) swaps runtime registration without validate", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("agg-inc-collector", vm.runtime, "agg-inc-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		obsCollector, err := newCollectingConsumer("agg-inc-obs-collector", vm.runtime, "agg-inc-obs")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(obsCollector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {
  inputs: "agg-inc-in",
  outputs: ["agg-inc-out"]
});

const ci = c.transform("Incrementalizer");

runtime.observe("aggregation^Δ", (e) => {
  runtime.publish("agg-inc-obs", [[{node: e.node.id}, 1]]);
  cancel();
});

publish("agg-inc-in", [[{id: 222}, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			for _, ev := range events {
				if got := zsetRowsByField(ev, "id"); len(got) > 0 && got[0] == "1:222" {
					return true
				}
			}
			return false
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Eventually(func() int {
			return len(obsCollector.Snapshot())
		}, 2*time.Second, 10*time.Millisecond).Should(BeNumerically(">", 0))
	})

	It("rejects transform with non-string name", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {inputs: "in", outputs: ["out"]});
c.transform(["Incrementalizer"]);
`
		err = runScript(vm, script)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("transformer name must be a string"))
	})

	It("compiles aggregate pipelines with binding objects", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("agg-bind-collector", vm.runtime, "agg-bind-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = aggregate.compile([
  [
    {"@inputs": ["Foo"]},
    {"@project": {"$.": "$."}},
    {"@output": "Bar"}
  ]
], {
  inputs: [{name: "agg-bind-in", logicalName: "Foo"}],
  outputs: [{name: "agg-bind-out", logicalName: "Bar"}]
});
c.validate();
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			_ = runScript(vm, `publish("agg-bind-in", [[{id: 22}, 1]]);`)
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(first.Name).To(Equal("agg-bind-out"))
		Expect(zsetRowsByField(first, "id")).To(Equal([]string{"1:22"}))
	})

	It("compiles SQL with string output name", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("sql-collector", vm.runtime, "sql-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
sql.table("t", "id int");
const c = sql.compile("select id from t", { output: "sql-out" });
c.validate();
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			_ = runScript(vm, `publish("t", [[{id: 31}, 1]]);`)
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(first.Name).To(Equal("sql-out"))
		Expect(zsetRowsByField(first, "id")).To(Equal([]string{"1:31"}))
	})

	It("auto-registers SQL circuits without explicit validate", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("sql-auto-collector", vm.runtime, "sql-auto-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
sql.table("sql_auto_t", "id int");
sql.compile("select id from sql_auto_t", { output: "sql-auto-out" });
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			_ = runScript(vm, `publish("sql_auto_t", [[{id: 88}, 1]]);`)
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(first.Name).To(Equal("sql-auto-out"))
		Expect(zsetRowsByField(first, "id")).To(Equal([]string{"1:88"}))
	})

	It("requires explicit SQL output option", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
sql.table("t_missing_out", "id int");
sql.compile("select id from t_missing_out");
`
		err = runScript(vm, script)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("options.output is required"))
	})

	It("compiles SQL with output binding object", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("sql-bind-collector", vm.runtime, "sql-bind-out")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
sql.table("t2", "id int");
const c = sql.compile("select id from t2", { output: { name: "sql-bind-out", logicalName: "output" } });
c.validate();
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			_ = runScript(vm, `publish("t2", [[{id: 41}, 1]]);`)
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(first.Name).To(Equal("sql-bind-out"))
		Expect(zsetRowsByField(first, "id")).To(Equal([]string{"1:41"}))
	})

	It("doc snippet basics: zset insert then update as delete plus insert", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("doc-basics-users", vm.runtime, "users")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
publish("users", [
  [{ id: 1, name: "alice" }, 1],
  [{ id: 2, name: "bob" }, 1]
]);

publish("users", [
  [{ id: 1, name: "alice" }, -1],
  [{ id: 2, name: "bob" }, -1],
  [{ id: 2, name: "bob-updated" }, 1]
]);
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() int {
			return len(collector.Snapshot())
		}, 2*time.Second, 10*time.Millisecond).Should(Equal(2))

		events := collector.Snapshot()
		Expect(zsetRowsByField(events[0], "id")).To(Equal([]string{"1:1", "1:2"}))
		Expect(zsetRowsByField(events[1], "id")).To(Equal([]string{"-1:1", "-1:2", "1:2"}))
	})

	It("doc snippet basics: aggregate select project incrementalized", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("doc-basics-result", vm.runtime, "result")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@select": {"@eq": ["$.metadata.namespace", "default"]}},
  {"@project": {name: "$.metadata.name", status: "$.status"}}
], {inputs: "pods", outputs: ["result"]});
c.transform("Incrementalizer").validate();
`
		Expect(runScript(vm, script)).To(Succeed())

		Expect(runScript(vm, `publish("pods", [[{metadata:{name:"pod-a",namespace:"default"},status:"Running"},1]]);`)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		rows := zsetRowsByField(first, "name")
		Expect(rows).To(Equal([]string{"1:pod-a"}))
	})

	It("doc snippet basics: aggregate distinct emits membership deltas", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("doc-basics-distinct", vm.runtime, "distinct-pods")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {name: "$.metadata.name"}},
  {"@distinct": null}
], {inputs: "pods", outputs: ["distinct-pods"]});
c.transform("Incrementalizer").validate();

publish("pods", [[{metadata:{name:"pod-a"}}, 1]]);
publish("pods", [[{metadata:{name:"pod-a"}}, 1]]);
publish("pods", [[{metadata:{name:"pod-a"}}, -1]]);
publish("pods", [[{metadata:{name:"pod-a"}}, -1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}

			hasAdd := false
			hasDel := false
			for _, ev := range events {
				for _, row := range zsetRowsByField(ev, "name") {
					if row == "1:pod-a" {
						hasAdd = true
					}
					if row == "-1:pod-a" {
						hasDel = true
					}
				}
			}

			return hasAdd && hasDel
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
	})

	It("doc snippet runtime: publish and subscribe shorthands", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("doc-runtime-topic", vm.runtime, "my-topic-copy")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
subscribe("my-topic", (entries) => {
  runtime.publish("my-topic-copy", entries);
  cancel();
});

publish("my-topic", [[{ id: 1, name: "alice" }, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(first.Name).To(Equal("my-topic-copy"))
		Expect(zsetRowsByField(first, "id")).To(Equal([]string{"1:1"}))
	})

	It("doc snippet runtime: processor with observer and runtime.observe", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("doc-runtime-obs", vm.runtime, "obs-events")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const c = aggregate.compile([
  {"@project": {"$.": "$."}}
], {inputs: "pods", outputs: ["result-observed"]});

c.observe((e) => {
  runtime.publish("obs-events", [[{via: "handle", node: e.node.id}, 1]]);
});

c.validate();

runtime.observe("aggregation", (e) => {
  runtime.publish("obs-events", [[{via: "runtime", node: e.node.id}, 1]]);
  cancel();
});

publish("pods", [[{id: 9}, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() int {
			return len(collector.Snapshot())
		}, 2*time.Second, 10*time.Millisecond).Should(BeNumerically(">=", 1))

		events := collector.Snapshot()
		allRows := make([]string, 0, len(events))
		for _, e := range events {
			allRows = append(allRows, zsetRowsByField(e, "via")...)
		}
		found := false
		for _, row := range allRows {
			if row == "1:runtime" {
				found = true
				break
			}
		}
		Expect(found).To(BeTrue())
	})

	It("doc snippet programming: SQL table and filter query", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("doc-sql-senior", vm.runtime, "senior-users")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
sql.table("users", "id INTEGER PRIMARY KEY, name TEXT, age INTEGER");
sql.compile("SELECT name, age FROM users WHERE age > 25", { output: "senior-users" }).transform("Incrementalizer").validate();
publish("users", [
  [{ id: 1, name: "alice", age: 30 }, 1],
  [{ id: 2, name: "bob", age: 22 }, 1]
]);
`
		Expect(runScript(vm, script)).To(Succeed())

		var first dbspruntime.Event
		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			first = events[0]
			return true
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		rows := zsetRowsByField(first, "name")
		Expect(rows).To(Equal([]string{"1:alice"}))
	})

	It("doc snippet programming: SQL compile accepts join query", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		script := `
sql.table("users", "id INTEGER PRIMARY KEY, name TEXT");
sql.table("orders", "id INTEGER PRIMARY KEY, user_id INTEGER, total REAL");
sql.compile("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id", { output: "user-orders" }).transform("Incrementalizer").validate();
`
		Expect(runScript(vm, script)).To(Succeed())
	})

	It("doc snippet programming: aggregate pipeline with explicit bindings", func() {
		vm, err := NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		defer vm.Close()

		collector, err := newCollectingConsumer("doc-agg-bindings", vm.runtime, "result-topic")
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(collector)).To(Succeed())

		script := `
const pipeline = [
  [{"@inputs": ["pods", "services"]},
   {"@join": {"@eq": ["$.pods.metadata.name", "$.services.metadata.name"]}},
   {"@project": {pod: "$.pods.metadata.name", svc: "$.services.metadata.name"}},
   {"@output": "output"}]
];

aggregate.compile(pipeline, {
  inputs: [
    {name: "pods-topic", logical: "pods"},
    {name: "svc-topic", logical: "services"}
  ],
  outputs: [{name: "result-topic", logical: "output"}]
}).transform("Incrementalizer").validate();

publish("pods-topic", [[{metadata:{name:"shared"}}, 1]]);
publish("svc-topic", [[{metadata:{name:"shared"}}, 1]]);
`
		Expect(runScript(vm, script)).To(Succeed())

		Eventually(func() bool {
			events := collector.Snapshot()
			if len(events) == 0 {
				return false
			}
			for _, ev := range events {
				if got := zsetRowsByField(ev, "pod"); len(got) > 0 && got[0] == "1:shared" {
					return true
				}
			}
			return false
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
	})
})
