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
})
