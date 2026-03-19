package runtime_test

import (
	"errors"
	"testing"

	"github.com/l7mp/dbsp/compiler"
	"github.com/l7mp/dbsp/compiler/aggregation"
	"github.com/l7mp/dbsp/datamodel/unstructured"
	"github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
)

func TestDeltaCircuitExecute(t *testing.T) {
	t.Parallel()

	q := mustCompileCircuitQuery(t)
	c, err := runtime.NewCircuit(runtime.CircuitConfig{
		Circuit:     q.Circuit,
		InputMap:    q.InputMap,
		OutputMap:   q.OutputMap,
		Incremental: true,
	})
	if err != nil {
		t.Fatalf("NewCircuit() error = %v", err)
	}

	delta := zset.New()
	doc := unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil)
	delta.Insert(doc, 1)

	outs, err := c.Execute(runtime.Input{Name: "Pod", Data: delta})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("outputs len = %d, want 1", len(outs))
	}
	if outs[0].Name != "output" {
		t.Fatalf("output name = %q, want output", outs[0].Name)
	}
	if !outs[0].Data.Equal(delta) {
		t.Fatal("output payload mismatch")
	}
}

func TestSnapshotCircuitExecute(t *testing.T) {
	t.Parallel()

	q := mustCompileCircuitQuery(t)
	c, err := runtime.NewCircuit(runtime.CircuitConfig{
		Circuit:     q.Circuit,
		InputMap:    q.InputMap,
		OutputMap:   q.OutputMap,
		Incremental: false,
	})
	if err != nil {
		t.Fatalf("NewCircuit() error = %v", err)
	}

	s1 := zset.New()
	s1.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-a"}}, nil), 1)

	outs, err := c.Execute(runtime.Input{Name: "Pod", Data: s1})
	if err != nil {
		t.Fatalf("Execute(first) error = %v", err)
	}
	if len(outs) != 1 || !outs[0].Data.Equal(s1) {
		t.Fatal("first snapshot output mismatch")
	}

	s2 := zset.New()
	s2.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod-b"}}, nil), 1)

	outs, err = c.Execute(runtime.Input{Name: "Pod", Data: s2})
	if err != nil {
		t.Fatalf("Execute(second) error = %v", err)
	}
	if len(outs) != 1 || !outs[0].Data.Equal(s2) {
		t.Fatal("second snapshot output mismatch")
	}
}

func TestCircuitRejectsUnknownInput(t *testing.T) {
	t.Parallel()

	q := mustCompileCircuitQuery(t)
	c, err := runtime.NewCircuit(runtime.CircuitConfig{
		Circuit:     q.Circuit,
		InputMap:    q.InputMap,
		OutputMap:   q.OutputMap,
		Incremental: true,
	})
	if err != nil {
		t.Fatalf("NewCircuit() error = %v", err)
	}

	_, err = c.Execute(runtime.Input{Name: "Deployment", Data: zset.New()})
	if !errors.Is(err, runtime.ErrUnknownInput) {
		t.Fatalf("Execute() error = %v, want ErrUnknownInput", err)
	}
}

func mustCompileCircuitQuery(t *testing.T) *compiler.Query {
	t.Helper()

	c := aggregation.New([]string{"Pod"}, []string{"output"})
	q, err := c.CompileString(`[{"@project":{"$.":"$."}}]`)
	if err != nil {
		t.Fatalf("CompileString() error = %v", err)
	}

	return q
}
