package misc

import (
	"context"
	"testing"
	"time"

	"github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
)

func TestPipeProducerForwardsInput(t *testing.T) {
	t.Parallel()

	in := make(chan runtime.Input, 1)
	p := NewPipeProducer(in)

	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotCh := make(chan runtime.Input, 1)
	p.SetInputHandler(func(_ context.Context, in runtime.Input) error {
		gotCh <- in
		return nil
	})

	go func() { errCh <- p.Start(ctx) }()

	want := runtime.Input{Name: "in", Data: zset.New()}
	in <- want

	select {
	case got := <-gotCh:
		if got.Name != want.Name {
			t.Fatalf("input name = %q, want %q", got.Name, want.Name)
		}
		if !got.Data.Equal(want.Data) {
			t.Fatal("input payload mismatch")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for forwarded input")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("producer exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not stop")
	}
}

func TestPipeConsumerForwardsOutput(t *testing.T) {
	t.Parallel()

	out := make(chan runtime.Output, 1)
	c := NewPipeConsumer(out)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()

	want := runtime.Output{Name: "out", Data: zset.New()}
	if err := c.Consume(ctx, want); err != nil {
		t.Fatalf("Consume() error = %v, want nil", err)
	}

	select {
	case got := <-out:
		if got.Name != want.Name {
			t.Fatalf("output name = %q, want %q", got.Name, want.Name)
		}
		if !got.Data.Equal(want.Data) {
			t.Fatal("output payload mismatch")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for forwarded output")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("consumer exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop")
	}
}
