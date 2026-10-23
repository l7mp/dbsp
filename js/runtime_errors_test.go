package js

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// failingComponent is a runnable whose Start fails at once.
type failingComponent struct {
	name string
	err  error
}

func (f *failingComponent) Name() string                { return f.name }
func (f *failingComponent) Start(context.Context) error { return f.err }

var _ = Describe("Critical runtime errors", func() {
	var vm *VM

	BeforeEach(func() {
		var err error
		vm, err = NewVM(logr.Discard())
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() { vm.Close() })

	// collect gathers the documents published to topic on the default
	// runtime.
	collect := func(topic string) *collectingConsumer {
		c, err := newCollectingConsumer(topic+"-collector", vm.runtime, topic)
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(c)).To(Succeed())
		return c
	}
	// field returns the first value at path among the collected documents.
	field := func(c *collectingConsumer, path string) func() any {
		return func() any {
			for _, ev := range c.Snapshot() {
				for _, e := range ev.Data.Entries() {
					if v, err := e.Document.GetField(path); err == nil {
						return v
					}
				}
			}
			return nil
		}
	}
	instance := func(name string) *runtimeInstance {
		vm.instMu.Lock()
		defer vm.instMu.Unlock()
		return vm.instances[name]
	}

	It("fail the script when a component of the default runtime fails", func() {
		Expect(vm.runtime.Add(&failingComponent{name: "broken", err: errors.New("boom")})).To(Succeed())
		done := make(chan error, 1)
		go func() { done <- vm.runEventLoop() }()

		var err error
		Eventually(done, 3*time.Second).Should(Receive(&err))
		Expect(err).To(MatchError(ContainSubstring("runtime failed: broken: boom")))
	})

	It("fail the script when the default onError handler throws", func() {
		Expect(runScript(vm, `runtime.onError(() => { throw new Error("escalate"); });`)).To(Succeed())
		emitter, err := newRuntimeErrorEmitter("noisy", vm.runtime, errors.New("benign"))
		Expect(err).NotTo(HaveOccurred())
		Expect(vm.runtime.Add(emitter)).To(Succeed())

		Eventually(vm.firstRuntimeError, 2*time.Second).Should(MatchError(ContainSubstring("runtime.onError callback failed")))
		Expect(vm.firstRuntimeError().Error()).To(ContainSubstring("escalate"))
	})

	It("stop only a created runtime and report it as critical", func() {
		crit := collect("crit")
		Expect(runScript(vm, `
const side = runtime.create("side");
side.onError((e) => publish("crit", [[e, 1]]));
side.start();
`)).To(Succeed())
		Expect(instance("side").rt.Add(&failingComponent{name: "broken", err: errors.New("boom")})).To(Succeed())

		Eventually(field(crit, "$.critical"), 2*time.Second).Should(Equal(true))
		Expect(field(crit, "$.origin")()).To(Equal("broken"))
		Expect(field(crit, "$.message")()).To(Equal("boom"))
		Expect(vm.ctx.Err()).NotTo(HaveOccurred())
		Expect(vm.firstRuntimeError()).NotTo(HaveOccurred())
		Expect(runScript(vm, `side.start();`)).To(MatchError(ContainSubstring(`runtime "side" failed`)))
	})

	It("stop only a created runtime whose onError handler throws", func() {
		crit := collect("crit2")
		Expect(runScript(vm, `
const side2 = runtime.create("side2");
side2.onError((e) => {
  if (!e.critical) throw new Error("escalate");
  publish("crit2", [[e, 1]]);
});
side2.start();
`)).To(Succeed())
		emitter, err := newRuntimeErrorEmitter("noisy", instance("side2").rt, errors.New("benign"))
		Expect(err).NotTo(HaveOccurred())
		Expect(instance("side2").rt.Add(emitter)).To(Succeed())

		Eventually(field(crit, "$.message"), 2*time.Second).Should(SatisfyAll(
			ContainSubstring("onError callback failed"), ContainSubstring("escalate")))
		Expect(field(crit, "$.origin")()).To(Equal("noisy"))
		Expect(vm.ctx.Err()).NotTo(HaveOccurred())
		Expect(vm.firstRuntimeError()).NotTo(HaveOccurred())
	})
})
