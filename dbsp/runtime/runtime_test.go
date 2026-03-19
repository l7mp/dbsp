package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/l7mp/dbsp/connectors/misc"
	"github.com/l7mp/dbsp/dbsp/compiler"
	"github.com/l7mp/dbsp/dbsp/compiler/aggregation"
	"github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	"github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
)

func TestRuntimeRequiresCircuit(t *testing.T) {
	t.Parallel()

	r := runtime.NewRuntime(runtime.Config{})
	err := r.Start(context.Background())
	if !errors.Is(err, runtime.ErrRuntimeMissingCircuit) {
		t.Fatalf("Start() error = %v, want ErrRuntimeMissingCircuit", err)
	}
}

func TestRuntimeAddOnlyOnce(t *testing.T) {
	t.Parallel()

	c1 := mustNewCircuit(t)
	c2 := mustNewCircuit(t)
	r := runtime.NewRuntime(runtime.Config{})

	if err := r.Add(c1); err != nil {
		t.Fatalf("Add(first) error = %v", err)
	}
	if err := r.Add(c2); err != nil {
		t.Fatalf("Add(second) error = %v", err)
	}
}

func TestRuntimeProducerTriggersCircuitInProducerContext(t *testing.T) {
	t.Parallel()

	c := mustNewCircuit(t)
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

	out := waitCollectedRuntimeOutput(t, consumer.collected)
	if out.Name != "output" {
		t.Fatalf("output name = %q, want output", out.Name)
	}

	want := zset.New()
	want.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
	if !out.Data.Equal(want) {
		t.Fatal("output payload mismatch")
	}

	if p.triggerGID == 0 || consumer.consumeGID == 0 {
		t.Fatalf("expected trace values to be captured")
	}
	if p.triggerGID != consumer.consumeGID {
		t.Fatalf("producer trace (%d) != consume trace (%d)", p.triggerGID, consumer.consumeGID)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runtime shutdown")
	}
}

func TestRuntimePropagatesConsumerError(t *testing.T) {
	t.Parallel()

	c := mustNewCircuit(t)
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
	if err == nil {
		t.Fatal("Start() error = nil, want non-nil")
	}
	if !errors.Is(err, consumer.err) {
		t.Fatalf("Start() error = %v, want wrapped consumer error", err)
	}
}

func TestRuntimeSumsOutputsFromMultipleCircuits(t *testing.T) {
	t.Parallel()

	c1 := mustNewCircuit(t)
	c2 := mustNewCircuit(t)
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

	out := waitCollectedRuntimeOutput(t, consumer.collected)
	want := zset.New()
	want.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 2)
	if !out.Data.Equal(want) {
		t.Fatal("aggregated output payload mismatch")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runtime shutdown")
	}
}

func TestRuntimeEndToEndWithPipeConnectors(t *testing.T) {
	t.Parallel()

	c := mustNewCircuit(t)
	in := make(chan runtime.Input, 1)
	out := make(chan runtime.Output, 1)

	p := misc.NewPipeProducer(in)
	consumer := misc.NewPipeConsumer(out)

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

	select {
	case got := <-out:
		if got.Name != "output" {
			t.Fatalf("output name = %q, want output", got.Name)
		}

		want := zset.New()
		want.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
		if !got.Data.Equal(want) {
			t.Fatal("output payload mismatch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runtime output")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runtime shutdown")
	}
}

func mustNewCircuit(t *testing.T) *runtime.Circuit {
	t.Helper()

	q := mustCompileRuntimeQuery(t)
	c, err := runtime.NewCircuit(runtime.CircuitConfig{
		Circuit:     q.Circuit,
		InputMap:    q.InputMap,
		OutputMap:   q.OutputMap,
		Incremental: true,
	})
	if err != nil {
		t.Fatalf("NewCircuit() error = %v", err)
	}
	return c
}

func mustCompileRuntimeQuery(t *testing.T) *compiler.Query {
	t.Helper()
	c := aggregation.New([]string{"Pod"}, []string{"output"})
	q, err := c.CompileString(`[{"@project":{"$.":"$."}}]`)
	if err != nil {
		t.Fatalf("CompileString() error = %v", err)
	}
	return q
}

func waitCollectedRuntimeOutput(t *testing.T, ch <-chan runtime.Output) runtime.Output {
	t.Helper()
	select {
	case out := <-ch:
		return out
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runtime output")
		return runtime.Output{}
	}
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

type traceContextKey struct{}
