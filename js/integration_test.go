package main

import (
	"context"
	"errors"
	"fmt"
	"os"
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
consumer("cancel-in", (entries) => {
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
  output: "obs-out"
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
  output: "obs-runtime-out"
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
  output: "console-out"
});

c.validate();
const p = producer("console-in");

console.log(c);
console.log(p);
console.log(runtime);

consumer("console-out", (entries) => {
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
  output: "agg-out"
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
  output: {name: "agg-bind-out", logicalName: "Bar"}
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
], {inputs: "pods", output: "result"});
c.incrementalize().validate();
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
], {inputs: "pods", output: "result-observed"});

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
sql.compile("SELECT name, age FROM users WHERE age > 25", { output: "senior-users" }).incrementalize().validate();
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
sql.compile("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id", { output: "user-orders" }).incrementalize().validate();
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
  output: {name: "result-topic", logical: "output"}
}).incrementalize().validate();

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
