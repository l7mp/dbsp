package runtime_test

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// failOp is a minimal operator whose Apply always returns an error.
// It is used to exercise the circuit's non-critical error reporting path.
type failOp struct{}

func (f *failOp) String() string                { return "fail" }
func (f *failOp) MarshalJSON() ([]byte, error)  { return []byte(`"fail"`), nil }
func (f *failOp) UnmarshalJSON([]byte) error    { return nil }
func (f *failOp) Kind() operator.Kind           { return operator.KindSelect }
func (f *failOp) Arity() int                    { return 1 }
func (f *failOp) Linearity() operator.Linearity { return operator.Linear }
func (f *failOp) Set(zset.ZSet)                 {}
func (f *failOp) Apply(...zset.ZSet) (zset.ZSet, error) {
	return zset.New(), errors.New("intentional operator failure")
}

// failingCircuitQuery builds a query with one failing operator node between
// the input and output.
func failingCircuitQuery() *compiler.Query {
	c := circuit.New("fail-circuit")
	_ = c.AddNode(circuit.Input("in"))
	_ = c.AddNode(circuit.Op("fail", &failOp{}))
	_ = c.AddNode(circuit.Output("out"))
	_ = c.AddEdge(circuit.NewEdge("in", "fail", 0))
	_ = c.AddEdge(circuit.NewEdge("fail", "out", 0))
	return &compiler.Query{
		Circuit:   c,
		InputMap:  map[string]string{"Pod": "in"},
		OutputMap: map[string]string{"output": "out"},
	}
}

var _ = Describe("Error reporting", func() {
	Describe("Runtime.ReportError", func() {
		It("sends to error channel when set", func() {
			errCh := make(chan runtime.Error, 4)
			rt := runtime.NewRuntime(logr.Discard())
			rt.SetErrorChannel(errCh)

			sentinel := errors.New("test error")
			rt.ReportError("my-component", sentinel)

			var ce runtime.Error
			Eventually(errCh, time.Second).Should(Receive(&ce))
			Expect(ce.Origin).To(Equal("my-component"))
			Expect(errors.Is(ce, sentinel)).To(BeTrue())
			Expect(ce.Error()).To(ContainSubstring("my-component"))
		})

		It("does not block when no channel is set", func() {
			rt := runtime.NewRuntime(logr.Discard())
			// Must not panic or block.
			Expect(func() {
				rt.ReportError("my-component", errors.New("test"))
			}).NotTo(Panic())
		})

		It("drops and logs when channel is full", func() {
			errCh := make(chan runtime.Error, 1) // capacity 1
			rt := runtime.NewRuntime(logr.Discard())
			rt.SetErrorChannel(errCh)

			rt.ReportError("c1", errors.New("first"))  // fills the channel
			rt.ReportError("c2", errors.New("second")) // dropped, must not block

			var ce runtime.Error
			Eventually(errCh, time.Second).Should(Receive(&ce))
			Expect(ce.Origin).To(Equal("c1"))
		})
	})

	Describe("Circuit error reporting", func() {
		It("reports executor errors to the error channel with the circuit name", func() {
			errCh := make(chan runtime.Error, 4)
			rt := runtime.NewRuntime(logr.Discard())
			rt.SetErrorChannel(errCh)

			q := failingCircuitQuery()
			c, err := runtime.NewCircuit("my-circuit", rt, q, logr.Discard())
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { c.Start(ctx) }() //nolint:errcheck

			// Publish an event that will trigger the failing operator.
			pub := rt.NewPublisher()
			Eventually(func() bool {
				_ = pub.Publish(runtime.Event{Name: "Pod", Data: zset.New()})
				select {
				case ce := <-errCh:
					return ce.Origin == "my-circuit"
				default:
					return false
				}
			}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
		})

		It("continues processing after a non-critical executor error", func() {
			// Build a circuit with one pass-through query to verify the circuit
			// survives the error and keeps running.
			errCh := make(chan runtime.Error, 16)
			rt := runtime.NewRuntime(logr.Discard())
			rt.SetErrorChannel(errCh)

			q := failingCircuitQuery()
			c, err := runtime.NewCircuit("survivor", rt, q, logr.Discard())
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			startDone := make(chan error, 1)
			go func() { startDone <- c.Start(ctx) }()

			pub := rt.NewPublisher()

			// Fire three events; each should produce an error on the channel
			// (circuit continues rather than dying after the first).
			for i := 0; i < 3; i++ {
				Eventually(func() bool {
					_ = pub.Publish(runtime.Event{Name: "Pod", Data: zset.New()})
					select {
					case <-errCh:
						return true
					default:
						return false
					}
				}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
			}

			// Cancel and confirm Start() returned nil (no fatal error).
			cancel()
			Eventually(startDone, time.Second).Should(Receive(BeNil()))
		})

		It("rejects duplicate circuit names", func() {
			rt := runtime.NewRuntime(logr.Discard())
			q := mustCompileCircuitQuery()
			c1, err := runtime.NewCircuit("shared-name", rt, q, logr.Discard())
			Expect(err).NotTo(HaveOccurred())
			Expect(rt.Add(c1)).To(Succeed())

			c2, err := runtime.NewCircuit("shared-name", rt, q, logr.Discard())
			Expect(err).NotTo(HaveOccurred())
			Expect(rt.Add(c2)).To(MatchError(ContainSubstring("already registered")))
		})
	})

	Describe("Critical errors", func() {
		It("returns invalid circuit creation errors directly", func() {
			errCh := make(chan runtime.Error, 4)
			rt := runtime.NewRuntime(logr.Discard())
			rt.SetErrorChannel(errCh)

			bad := circuit.New("bad")
			Expect(bad.AddNode(circuit.Input("in"))).To(Succeed())
			Expect(bad.AddNode(circuit.Output("out"))).To(Succeed())
			Expect(bad.AddEdge(circuit.NewEdge("in", "out", 0))).To(Succeed())
			Expect(bad.AddEdge(circuit.NewEdge("out", "in", 0))).To(Succeed())

			_, err := runtime.NewCircuit("bad-circuit", rt, &compiler.Query{
				Circuit:   bad,
				InputMap:  map[string]string{"Pod": "in"},
				OutputMap: map[string]string{"output": "out"},
			}, logr.Discard())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid circuit"))

			Consistently(errCh, 200*time.Millisecond).ShouldNot(Receive())
		})

		It("returns duplicate registration errors directly", func() {
			errCh := make(chan runtime.Error, 4)
			rt := runtime.NewRuntime(logr.Discard())
			rt.SetErrorChannel(errCh)

			p1 := newCriticalFailProducer("dup", nil)
			p2 := newCriticalFailProducer("dup", nil)

			Expect(rt.Add(p1)).To(Succeed())
			Expect(rt.Add(p2)).To(MatchError(ContainSubstring("already registered")))

			Consistently(errCh, 200*time.Millisecond).ShouldNot(Receive())
		})

		It("returns producer Start errors via Runtime.Start", func() {
			errCh := make(chan runtime.Error, 4)
			rt := runtime.NewRuntime(logr.Discard())
			rt.SetErrorChannel(errCh)

			sentinel := errors.New("producer start failed")
			p := newCriticalFailProducer("bad-producer", sentinel)
			Expect(rt.Add(p)).To(Succeed())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			startDone := make(chan error, 1)
			go func() { startDone <- rt.Start(ctx) }()

			Eventually(p.started, time.Second).Should(BeClosed())
			Consistently(errCh, 200*time.Millisecond).ShouldNot(Receive())

			cancel()
			Eventually(startDone, time.Second).Should(Receive(MatchError(sentinel)))
		})

		It("returns consumer Start errors via Runtime.Start", func() {
			errCh := make(chan runtime.Error, 4)
			rt := runtime.NewRuntime(logr.Discard())
			rt.SetErrorChannel(errCh)

			sentinel := errors.New("consumer start failed")
			c := newCriticalFailConsumer("bad-consumer", sentinel)
			Expect(rt.Add(c)).To(Succeed())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			startDone := make(chan error, 1)
			go func() { startDone <- rt.Start(ctx) }()

			Eventually(c.started, time.Second).Should(BeClosed())
			Consistently(errCh, 200*time.Millisecond).ShouldNot(Receive())

			cancel()
			Eventually(startDone, time.Second).Should(Receive(MatchError(sentinel)))
		})
	})
})

type criticalFailProducer struct {
	name    string
	err     error
	started chan struct{}
}

func newCriticalFailProducer(name string, err error) *criticalFailProducer {
	return &criticalFailProducer{name: name, err: err, started: make(chan struct{})}
}

func (p *criticalFailProducer) Name() string { return p.name }

func (p *criticalFailProducer) Start(ctx context.Context) error {
	close(p.started)
	return p.err
}

func (p *criticalFailProducer) Publish(event runtime.Event) error { return nil }

type criticalFailConsumer struct {
	name    string
	err     error
	started chan struct{}
	ch      chan runtime.Event
}

func newCriticalFailConsumer(name string, err error) *criticalFailConsumer {
	return &criticalFailConsumer{name: name, err: err, started: make(chan struct{}), ch: make(chan runtime.Event)}
}

func (c *criticalFailConsumer) Name() string { return c.name }

func (c *criticalFailConsumer) Start(ctx context.Context) error {
	close(c.started)
	return c.err
}

func (c *criticalFailConsumer) Subscribe(topic string) {}

func (c *criticalFailConsumer) Unsubscribe(topic string) {}

func (c *criticalFailConsumer) GetChannel() <-chan runtime.Event { return c.ch }
