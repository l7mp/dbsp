package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/l7mp/dbsp/compiler"
	"github.com/l7mp/dbsp/compiler/aggregation"
	"github.com/l7mp/dbsp/datamodel/unstructured"
	"github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
)

func TestDeltaCircuitImmediateTrigger(t *testing.T) {
	t.Parallel()

	q := mustCompileQuery(t, []string{"Pod"}, []string{"output"}, `[{"@project":{"$.":"$."}}]`)
	r, err := runtime.NewCircuit(runtime.CircuitConfig{
		Circuit:     q.Circuit,
		InputMap:    q.InputMap,
		OutputMap:   q.OutputMap,
		Incremental: true,
	})
	if err != nil {
		t.Fatalf("NewCircuit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Start(ctx)
	}()

	delta := zset.New()
	doc := unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil)
	delta.Insert(doc, 1)

	in := r.Input()
	in <- runtime.Input{Name: "Pod", Data: delta}

	select {
	case out := <-r.Output():
		if out.Name != "output" {
			t.Fatalf("output name = %q, want %q", out.Name, "output")
		}
		if !out.Data.Equal(delta) {
			t.Fatalf("output data does not match input delta")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runtime output")
	}

	close(in)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runtime shutdown")
	}
}

func TestSnapshotCircuitImmediateTrigger(t *testing.T) {
	t.Parallel()

	q := mustCompileQuery(t, []string{"Pod"}, []string{"output"}, `[{"@project":{"$.":"$."}}]`)
	r, err := runtime.NewCircuit(runtime.CircuitConfig{
		Circuit:     q.Circuit,
		InputMap:    q.InputMap,
		OutputMap:   q.OutputMap,
		Incremental: false,
	})
	if err != nil {
		t.Fatalf("NewCircuit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Start(ctx)
	}()

	in := r.Input()

	s1 := zset.New()
	s1.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
	in <- runtime.Input{Name: "Pod", Data: s1}

	select {
	case out := <-r.Output():
		if !out.Data.Equal(s1) {
			t.Fatalf("first output does not match snapshot input")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first snapshot output")
	}

	s2 := zset.New()
	s2.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-b"}}, nil), 1)
	in <- runtime.Input{Name: "Pod", Data: s2}

	select {
	case out := <-r.Output():
		if !out.Data.Equal(s2) {
			t.Fatalf("second output does not match snapshot input")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second snapshot output")
	}

	close(in)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for runtime shutdown")
	}
}

func TestCircuitRejectsUnknownInput(t *testing.T) {
	t.Parallel()

	q := mustCompileQuery(t, []string{"Pod"}, []string{"output"}, `[{"@project":{"$.":"$."}}]`)
	r, err := runtime.NewCircuit(runtime.CircuitConfig{
		Circuit:     q.Circuit,
		InputMap:    q.InputMap,
		OutputMap:   q.OutputMap,
		Incremental: true,
	})
	if err != nil {
		t.Fatalf("NewCircuit() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Start(ctx)
	}()

	data := zset.New()
	data.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)
	r.Input() <- runtime.Input{Name: "Deployment", Data: data}

	select {
	case err := <-errCh:
		if !errors.Is(err, runtime.ErrUnknownInput) {
			t.Fatalf("Start() error = %v, want ErrUnknownInput", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for unknown input error")
	}
}

func mustCompileQuery(t *testing.T, sources, outputs []string, program string) *compiler.Query {
	t.Helper()

	c := aggregation.New(sources, outputs)
	q, err := c.CompileString(program)
	if err != nil {
		t.Fatalf("CompileString() error = %v", err)
	}

	return q
}
