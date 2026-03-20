package runtime_test

import (
	"context"
	"errors"
	"time"

	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/compiler/aggregation"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Runtime", func() {
	It("requires at least one circuit", func() {
		r := runtime.NewRuntime(runtime.Config{})
		err := r.Start(context.Background())
		Expect(errors.Is(err, runtime.ErrRuntimeMissingCircuit)).To(BeTrue())
	})

	It("accepts multiple circuits via Add", func() {
		c1 := mustNewCircuit()
		c2 := mustNewCircuit()
		r := runtime.NewRuntime(runtime.Config{})

		Expect(r.Add(c1)).To(Succeed())
		Expect(r.Add(c2)).To(Succeed())
	})

	It("runs consumer in producer-triggered context", func() {
		c := mustNewCircuit()
		p := &scriptedProducer{}
		consumer := newCollectingConsumer()

		r := runtime.NewRuntime(runtime.Config{
			Circuit:   c,
			Producers: []runtime.Producer{p},
			Consumers: []runtime.Consumer{consumer},
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- r.Start(ctx) }()

		out := waitCollectedRuntimeOutput(consumer.collected)
		Expect(out.Name).To(Equal("output"))

		want := zset.New()
		want.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
		Expect(out.Data.Equal(want)).To(BeTrue())

		Expect(p.triggerGID).NotTo(BeZero())
		Expect(consumer.consumeGID).NotTo(BeZero())
		Expect(p.triggerGID).To(Equal(consumer.consumeGID))

		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))
	})

	It("propagates consumer errors", func() {
		c := mustNewCircuit()
		p := &scriptedProducer{}
		consumer := &failingConsumer{err: errors.New("boom")}

		r := runtime.NewRuntime(runtime.Config{
			Circuit:   c,
			Producers: []runtime.Producer{p},
			Consumers: []runtime.Consumer{consumer},
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := r.Start(ctx)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, consumer.err)).To(BeTrue())
	})

	It("sums outputs from multiple circuits", func() {
		c1 := mustNewCircuit()
		c2 := mustNewCircuit()
		p := &scriptedProducer{}
		consumer := newCollectingConsumer()

		r := runtime.NewRuntime(runtime.Config{
			Circuits:  []*runtime.Circuit{c1, c2},
			Producers: []runtime.Producer{p},
			Consumers: []runtime.Consumer{consumer},
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- r.Start(ctx) }()

		out := waitCollectedRuntimeOutput(consumer.collected)
		want := zset.New()
		want.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 2)
		Expect(out.Data.Equal(want)).To(BeTrue())

		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))
	})

	It("supports end-to-end pipe producer and consumer", func() {
		c := mustNewCircuit()
		in := make(chan runtime.Input, 1)
		out := make(chan runtime.Output, 1)

		p := newPipeProducer(in)
		consumer := newPipeConsumer(out)

		r := runtime.NewRuntime(runtime.Config{
			Circuit:   c,
			Producers: []runtime.Producer{p},
			Consumers: []runtime.Consumer{consumer},
		})

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- r.Start(ctx) }()

		delta := zset.New()
		delta.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
		in <- runtime.Input{Name: "Pod", Data: delta}

		var got runtime.Output
		Eventually(out, 2*time.Second).Should(Receive(&got))
		Expect(got.Name).To(Equal("output"))

		want := zset.New()
		want.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
		Expect(got.Data.Equal(want)).To(BeTrue())

		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))
	})
})

func mustNewCircuit() *runtime.Circuit {
	q := mustCompileRuntimeQuery()
	c, err := runtime.NewCircuit(runtime.CircuitConfig{
		Circuit:     q.Circuit,
		InputMap:    q.InputMap,
		OutputMap:   q.OutputMap,
		Incremental: true,
	})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return c
}

func mustCompileRuntimeQuery() *compiler.Query {
	c := aggregation.New([]string{"Pod"}, []string{"output"})
	q, err := c.CompileString(`[{"@project":{"$.":"$."}}]`)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return q
}

func waitCollectedRuntimeOutput(ch <-chan runtime.Output) runtime.Output {
	var out runtime.Output
	EventuallyWithOffset(1, ch, 2*time.Second).Should(Receive(&out))
	return out
}

type scriptedProducer struct {
	h          runtime.InputHandler
	triggerGID uint64
}

func (p *scriptedProducer) SetInputHandler(h runtime.InputHandler) {
	p.h = h
}

func (p *scriptedProducer) Start(ctx context.Context) error {
	if p.h == nil {
		return errors.New("missing input handler")
	}

	delta := zset.New()
	delta.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)

	traceID := uint64(1)
	ctx = context.WithValue(ctx, traceContextKey{}, traceID)
	p.triggerGID = traceID
	if err := p.h(ctx, runtime.Input{Name: "Pod", Data: delta}); err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

type collectingConsumer struct {
	consumeGID uint64
	collected  chan runtime.Output
}

func newCollectingConsumer() *collectingConsumer {
	return &collectingConsumer{collected: make(chan runtime.Output, 8)}
}

func (c *collectingConsumer) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (c *collectingConsumer) Consume(ctx context.Context, out runtime.Output) error {
	v := ctx.Value(traceContextKey{})
	if id, ok := v.(uint64); ok {
		c.consumeGID = id
	}
	c.collected <- out
	return nil
}

type failingConsumer struct {
	err error
}

func (c *failingConsumer) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (c *failingConsumer) Consume(_ context.Context, _ runtime.Output) error {
	return c.err
}

type pipeProducer struct {
	ch chan runtime.Input
	h  runtime.InputHandler
}

func newPipeProducer(ch chan runtime.Input) *pipeProducer {
	return &pipeProducer{ch: ch}
}

func (p *pipeProducer) SetInputHandler(h runtime.InputHandler) { p.h = h }

func (p *pipeProducer) Start(ctx context.Context) error {
	if p.h == nil {
		return errors.New("missing input handler")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case in := <-p.ch:
			if err := p.h(ctx, in); err != nil {
				return err
			}
		}
	}
}

type pipeConsumer struct{ ch chan runtime.Output }

func newPipeConsumer(ch chan runtime.Output) *pipeConsumer { return &pipeConsumer{ch: ch} }

func (c *pipeConsumer) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (c *pipeConsumer) Consume(_ context.Context, out runtime.Output) error {
	c.ch <- out
	return nil
}

type traceContextKey struct{}
