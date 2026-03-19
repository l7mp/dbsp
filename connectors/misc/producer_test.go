package misc

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	dbspruntime "github.com/l7mp/dbsp/dbsp/runtime"
)

func TestOneShotProducerEmitsExactlyOnce(t *testing.T) {
	p, err := NewOneShotProducer(OneShotConfig{
		InputName:  "in",
		TriggerGVK: schema.GroupVersionKind{Group: "test.io", Version: "v1", Kind: "OneShotTrigger"},
	})
	if err != nil {
		t.Fatalf("NewOneShotProducer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	count := 0
	p.SetInputHandler(func(_ context.Context, in dbspruntime.Input) error {
		mu.Lock()
		defer mu.Unlock()
		if in.Name != "in" {
			t.Fatalf("unexpected input name: %s", in.Name)
		}
		count++
		return nil
	})

	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	time.Sleep(40 * time.Millisecond)
	mu.Lock()
	got := count
	mu.Unlock()
	if got != 1 {
		t.Fatalf("expected exactly one event, got %d", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("producer exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not stop")
	}
}

func TestPeriodicProducerEmitsRepeatedly(t *testing.T) {
	p, err := NewPeriodicProducer(PeriodicConfig{
		InputName:  "in",
		TriggerGVK: schema.GroupVersionKind{Group: "test.io", Version: "v1", Kind: "PeriodicTrigger"},
		Period:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPeriodicProducer failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	count := 0
	p.SetInputHandler(func(_ context.Context, in dbspruntime.Input) error {
		mu.Lock()
		defer mu.Unlock()
		if in.Name != "in" {
			t.Fatalf("unexpected input name: %s", in.Name)
		}
		count++
		return nil
	})

	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	time.Sleep(90 * time.Millisecond)
	mu.Lock()
	got := count
	mu.Unlock()
	if got < 3 {
		t.Fatalf("expected at least 3 periodic events, got %d", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("producer exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not stop")
	}
}
