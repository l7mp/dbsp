package runtime_test

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/compiler/aggregation"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ runtime.Processor = (*runtime.Circuit)(nil)

var _ = Describe("Circuit", func() {
	It("executes incremental mode on delta input", func() {
		q := mustCompileCircuitQuery()
		rt := runtime.NewRuntime(logr.Discard())
		c, err := runtime.NewCircuit("test-circuit", rt, q, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		delta := zset.New()
		doc := unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil)
		delta.Insert(doc, 1)

		outs, err := c.Execute(runtime.Event{Name: "Pod", Data: delta})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs).To(HaveLen(1))
		Expect(outs[0].Name).To(Equal("output"))
		Expect(outs[0].Data.Equal(delta)).To(BeTrue())
	})

	It("subscribes inputs and publishes outputs", func() {
		q := mustCompileCircuitQuery()
		rt := runtime.NewRuntime(logr.Discard())
		c, err := runtime.NewCircuit("test-circuit", rt, q, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		consumer := rt.NewSubscriber()
		consumer.Subscribe("output")
		defer consumer.Unsubscribe("output")

		Expect(rt.Add(c)).To(Succeed())
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()
		Eventually(func() error {
			probe := rt.NewPublisher()
			return probe.Publish(runtime.Event{Name: "Pod", Data: zset.New()})
		}, time.Second).Should(Succeed())

		var out runtime.Event
		Eventually(func() bool {
			producer := rt.NewPublisher()
			delta := zset.New()
			delta.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
			_ = producer.Publish(runtime.Event{Name: "Pod", Data: delta})
			select {
			case out = <-consumer.GetChannel():
				return true
			default:
				return false
			}
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())
		Expect(out.Name).To(Equal("output"))

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})

	It("broadcasts across overlapping producers and consumers", func() {
		rt := runtime.NewRuntime(logr.Discard())

		q1 := newBroadcasterQuery("b1", "p1", "p2", "x", "y")
		q2 := newBroadcasterQuery("b2", "p2", "p3", "y", "z")

		c1, err := runtime.NewCircuit("b1", rt, q1, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		c2, err := runtime.NewCircuit("b2", rt, q2, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		cons1 := newCountingConsumer("c1", rt.NewSubscriber(), []string{"x", "y"})
		cons2 := newCountingConsumer("c2", rt.NewSubscriber(), []string{"y", "z"})
		cons3 := newCountingConsumer("c3", rt.NewSubscriber(), []string{"x", "z"})
		prodStart := make(chan struct{})

		prod1 := newProducerRunnable("p1", rt.NewPublisher(), prodStart, runtime.Event{Name: "p1", Data: oneDocZSet("p1")})
		prod2 := newProducerRunnable("p2", rt.NewPublisher(), prodStart, runtime.Event{Name: "p2", Data: oneDocZSet("p2")})
		prod3 := newProducerRunnable("p3", rt.NewPublisher(), prodStart, runtime.Event{Name: "p3", Data: oneDocZSet("p3")})

		Expect(rt.Add(c1)).To(Succeed())
		Expect(rt.Add(c2)).To(Succeed())
		rt.Add(cons1)
		rt.Add(cons2)
		rt.Add(cons3)
		rt.Add(prod1)
		rt.Add(prod2)
		rt.Add(prod3)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()
		time.Sleep(100 * time.Millisecond)
		close(prodStart)

		Eventually(func() int { return cons1.total() }, 2*time.Second, 10*time.Millisecond).Should(Equal(6))
		Eventually(func() int { return cons2.total() }, 2*time.Second, 10*time.Millisecond).Should(Equal(6))
		Eventually(func() int { return cons3.total() }, 2*time.Second, 10*time.Millisecond).Should(Equal(4))

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})

	It("broadcasts bursts with deterministic totals", func() {
		rt := runtime.NewRuntime(logr.Discard())

		q1 := newBroadcasterQuery("b1", "p1", "p2", "x", "y")
		q2 := newBroadcasterQuery("b2", "p2", "p3", "y", "z")

		c1, err := runtime.NewCircuit("b1", rt, q1, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		c2, err := runtime.NewCircuit("b2", rt, q2, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		cons1 := newCountingConsumer("c1", rt.NewSubscriber(), []string{"x", "y"})
		cons2 := newCountingConsumer("c2", rt.NewSubscriber(), []string{"y", "z"})
		cons3 := newCountingConsumer("c3", rt.NewSubscriber(), []string{"x", "z"})
		prodStart := make(chan struct{})

		const n = 20
		prod1 := newProducerBurstRunnable("p1", rt.NewPublisher(), prodStart, "p1", n)
		prod2 := newProducerBurstRunnable("p2", rt.NewPublisher(), prodStart, "p2", n)
		prod3 := newProducerBurstRunnable("p3", rt.NewPublisher(), prodStart, "p3", n)

		Expect(rt.Add(c1)).To(Succeed())
		Expect(rt.Add(c2)).To(Succeed())
		rt.Add(cons1)
		rt.Add(cons2)
		rt.Add(cons3)
		rt.Add(prod1)
		rt.Add(prod2)
		rt.Add(prod3)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()
		time.Sleep(100 * time.Millisecond)
		close(prodStart)

		Eventually(func() int { return cons1.total() }, 3*time.Second, 10*time.Millisecond).Should(Equal(6 * n))
		Eventually(func() int { return cons2.total() }, 3*time.Second, 10*time.Millisecond).Should(Equal(6 * n))
		Eventually(func() int { return cons3.total() }, 3*time.Second, 10*time.Millisecond).Should(Equal(4 * n))

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})

	It("supports runtime add and stop of circuits", func() {
		rt := runtime.NewRuntime(logr.Discard())

		q1 := newBroadcasterQuery("b1", "p1", "p2", "x", "y")
		q2 := newBroadcasterQuery("b2", "p2", "p3", "y", "z")
		c1, err := runtime.NewCircuit("b1", rt, q1, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		c2, err := runtime.NewCircuit("b2", rt, q2, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		cons1 := newCountingConsumer("c1", rt.NewSubscriber(), []string{"x", "y"})
		cons2 := newCountingConsumer("c2", rt.NewSubscriber(), []string{"y", "z"})
		cons3 := newCountingConsumer("c3", rt.NewSubscriber(), []string{"x", "z"})

		rt.Add(cons1)
		rt.Add(cons2)
		rt.Add(cons3)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()

		pub := rt.NewPublisher()
		Expect(pub.Publish(runtime.Event{Name: "p2", Data: oneDocZSet("none")})).NotTo(HaveOccurred())
		Consistently(func() int { return cons1.total() + cons2.total() + cons3.total() }, 200*time.Millisecond, 20*time.Millisecond).Should(Equal(0))

		Expect(rt.Add(c1)).To(Succeed())
		time.Sleep(100 * time.Millisecond)
		cons1.reset()
		cons2.reset()
		cons3.reset()

		Expect(pub.Publish(runtime.Event{Name: "p2", Data: oneDocZSet("c1")})).NotTo(HaveOccurred())
		Eventually(func() int { return cons1.total() }, time.Second, 10*time.Millisecond).Should(Equal(2))
		Eventually(func() int { return cons2.total() }, time.Second, 10*time.Millisecond).Should(Equal(1))
		Eventually(func() int { return cons3.total() }, time.Second, 10*time.Millisecond).Should(Equal(1))

		Expect(rt.Add(c2)).To(Succeed())
		time.Sleep(100 * time.Millisecond)
		cons1.reset()
		cons2.reset()
		cons3.reset()

		Expect(pub.Publish(runtime.Event{Name: "p2", Data: oneDocZSet("both")})).NotTo(HaveOccurred())
		Eventually(func() int { return cons1.total() }, time.Second, 10*time.Millisecond).Should(Equal(3))
		Eventually(func() int { return cons2.total() }, time.Second, 10*time.Millisecond).Should(Equal(3))
		Eventually(func() int { return cons3.total() }, time.Second, 10*time.Millisecond).Should(Equal(2))

		rt.Stop(c1)
		time.Sleep(100 * time.Millisecond)
		cons1.reset()
		cons2.reset()
		cons3.reset()
		Expect(pub.Publish(runtime.Event{Name: "p2", Data: oneDocZSet("c2-only")})).NotTo(HaveOccurred())
		Eventually(func() int { return cons1.total() }, time.Second, 10*time.Millisecond).Should(Equal(1))
		Eventually(func() int { return cons2.total() }, time.Second, 10*time.Millisecond).Should(Equal(2))
		Eventually(func() int { return cons3.total() }, time.Second, 10*time.Millisecond).Should(Equal(1))

		rt.Stop(c2)
		time.Sleep(100 * time.Millisecond)
		cons1.reset()
		cons2.reset()
		cons3.reset()
		_ = pub.Publish(runtime.Event{Name: "p3", Data: oneDocZSet("none-again")})
		Consistently(func() int { return cons1.total() + cons2.total() + cons3.total() }, 300*time.Millisecond, 20*time.Millisecond).Should(Equal(0))

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})

	It("sets circuit observers through runtime", func() {
		q := mustCompileCircuitQuery()
		rt := runtime.NewRuntime(logr.Discard())
		c, err := runtime.NewCircuit("test-circuit", rt, q, logr.Discard())
		Expect(err).NotTo(HaveOccurred())

		consumer := rt.NewSubscriber()
		consumer.Subscribe("output")
		defer consumer.Unsubscribe("output")

		var called atomic.Int64
		ok := rt.SetCircuitObserver("test-circuit", func(node *circuit.Node, values map[string]zset.ZSet, schedule []string, position int) {
			called.Add(1)
		})
		Expect(ok).To(BeFalse())

		Expect(rt.Add(c)).To(Succeed())
		ok = rt.SetCircuitObserver("test-circuit", func(node *circuit.Node, values map[string]zset.ZSet, schedule []string, position int) {
			called.Add(1)
		})
		Expect(ok).To(BeTrue())

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()

		Eventually(func() bool {
			delta := zset.New()
			delta.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
			if err := rt.NewPublisher().Publish(runtime.Event{Name: "Pod", Data: delta}); err != nil {
				return false
			}
			select {
			case out := <-consumer.GetChannel():
				return out.Name == "output"
			default:
				return false
			}
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Eventually(called.Load, time.Second, 10*time.Millisecond).Should(BeNumerically(">", 0))

		rt.Stop(c)
		Expect(rt.SetCircuitObserver("test-circuit", nil)).To(BeFalse())

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})
})

func mustCompileCircuitQuery() *compiler.Query {
	c := aggregation.New(
		[]aggregation.Binding{{Name: "Pod", Logical: "Pod"}},
		[]aggregation.Binding{{Name: "output", Logical: "output"}},
	)
	q, err := c.CompileString(`[{"@project":{"$.":"$."}}]`)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return q
}

func newBroadcasterQuery(name, inA, inB, outA, outB string) *compiler.Query {
	c := circuit.New(name)
	Expect(c.AddNode(circuit.Input("in_a"))).To(Succeed())
	Expect(c.AddNode(circuit.Input("in_b"))).To(Succeed())
	Expect(c.AddNode(circuit.Op("merge", operator.NewPlus()))).To(Succeed())
	Expect(c.AddNode(circuit.Output("out_a"))).To(Succeed())
	Expect(c.AddNode(circuit.Output("out_b"))).To(Succeed())
	Expect(c.AddEdge(circuit.NewEdge("in_a", "merge", 0))).To(Succeed())
	Expect(c.AddEdge(circuit.NewEdge("in_b", "merge", 1))).To(Succeed())
	Expect(c.AddEdge(circuit.NewEdge("merge", "out_a", 0))).To(Succeed())
	Expect(c.AddEdge(circuit.NewEdge("merge", "out_b", 0))).To(Succeed())

	return &compiler.Query{
		Circuit: c,
		InputMap: map[string]string{
			inA: "in_a",
			inB: "in_b",
		},
		OutputMap: map[string]string{
			outA: "out_a",
			outB: "out_b",
		},
	}
}

func oneDocZSet(tag string) zset.ZSet {
	z := zset.New()
	z.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": tag}}, nil), 1)
	return z
}

type producerRunnable struct {
	name string
	runtime.Publisher
	start  <-chan struct{}
	events []runtime.Event
}

func newProducerRunnable(name string, pub runtime.Publisher, start <-chan struct{}, events ...runtime.Event) *producerRunnable {
	return &producerRunnable{name: name, Publisher: pub, start: start, events: events}
}

func (p *producerRunnable) Name() string { return p.name }

func (p *producerRunnable) Start(context.Context) error {
	if p.start != nil {
		<-p.start
	}
	for _, e := range p.events {
		if err := p.Publish(e); err != nil {
			return err
		}
	}
	return nil
}

type producerBurstRunnable struct {
	name string
	runtime.Publisher
	start <-chan struct{}
	topic string
	n     int
}

func newProducerBurstRunnable(name string, pub runtime.Publisher, start <-chan struct{}, topic string, n int) *producerBurstRunnable {
	return &producerBurstRunnable{name: name, Publisher: pub, start: start, topic: topic, n: n}
}

func (p *producerBurstRunnable) Name() string { return p.name }
func (p *producerBurstRunnable) Start(context.Context) error {
	if p.start != nil {
		<-p.start
	}
	for i := 0; i < p.n; i++ {
		if err := p.Publish(runtime.Event{Name: p.topic, Data: oneDocZSet(p.topic)}); err != nil {
			return err
		}
	}
	return nil
}

type countingConsumer struct {
	name string
	runtime.Subscriber
	topics []string

	mu     sync.Mutex
	counts map[string]int
}

func newCountingConsumer(name string, sub runtime.Subscriber, topics []string) *countingConsumer {
	return &countingConsumer{name: name, Subscriber: sub, topics: topics, counts: map[string]int{}}
}

func (c *countingConsumer) Name() string { return c.name }
func (c *countingConsumer) Start(ctx context.Context) error {
	for _, t := range c.topics {
		c.Subscribe(t)
	}
	stop := context.AfterFunc(ctx, c.UnsubscribeAll)
	defer stop()
	for {
		e, ok := c.Next()
		if !ok {
			return nil
		}
		c.mu.Lock()
		c.counts[e.Name]++
		c.mu.Unlock()
	}
}

func (c *countingConsumer) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := 0
	for _, n := range c.counts {
		t += n
	}
	return t
}

func (c *countingConsumer) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = map[string]int{}
}
