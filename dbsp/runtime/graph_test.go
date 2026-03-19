package runtime_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/internal/testutils"
)

func TestGraphFanoutInputToMultipleProcessors(t *testing.T) {
	t.Parallel()

	p1 := newTestProcessor(func(in runtime.Input) []runtime.Output {
		return []runtime.Output{{Name: "out", Data: in.Data}}
	})
	p2 := newTestProcessor(func(in runtime.Input) []runtime.Output {
		return []runtime.Output{{Name: "out", Data: in.Data}}
	})

	g, err := runtime.NewGraph(runtime.GraphConfig{
		Processors: map[string]runtime.Processor{"p1": p1, "p2": p2},
		InputBindings: map[string][]runtime.Port{
			"orders": {
				{Processor: "p1", Name: "in"},
				{Processor: "p2", Name: "in"},
			},
		},
		OutputBindings: map[string][]runtime.Port{
			"p1out": {{Processor: "p1", Name: "out"}},
			"p2out": {{Processor: "p2", Name: "out"}},
		},
	})
	if err != nil {
		t.Fatalf("NewGraph() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.Start(ctx)
	}()

	delta := zset.New()
	delta.Insert(testutils.StringElem("x"), 1)
	g.Input() <- runtime.Input{Name: "orders", Data: delta}

	outs := collectOutputs(t, g.Output(), 2)
	names := []string{outs[0].Name, outs[1].Name}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "p1out" || names[1] != "p2out" {
		t.Fatalf("output names = %v, want [p1out p2out]", names)
	}

	for _, out := range outs {
		if !out.Data.Equal(delta) {
			t.Fatalf("output %q payload mismatch", out.Name)
		}
	}

	close(g.Input())
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for graph shutdown")
	}
}

func TestGraphInternalLinkPipeline(t *testing.T) {
	t.Parallel()

	p1 := newTestProcessor(func(in runtime.Input) []runtime.Output {
		return []runtime.Output{{Name: "mid", Data: in.Data}}
	})
	p2 := newTestProcessor(func(in runtime.Input) []runtime.Output {
		return []runtime.Output{{Name: "final", Data: in.Data}}
	})

	g, err := runtime.NewGraph(runtime.GraphConfig{
		Processors: map[string]runtime.Processor{"p1": p1, "p2": p2},
		InputBindings: map[string][]runtime.Port{
			"in": {{Processor: "p1", Name: "in"}},
		},
		Links: map[runtime.Port][]runtime.Port{
			{Processor: "p1", Name: "mid"}: {{Processor: "p2", Name: "in"}},
		},
		OutputBindings: map[string][]runtime.Port{
			"out": {{Processor: "p2", Name: "final"}},
		},
	})
	if err != nil {
		t.Fatalf("NewGraph() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.Start(ctx)
	}()

	delta := zset.New()
	delta.Insert(testutils.StringElem("a"), 1)
	g.Input() <- runtime.Input{Name: "in", Data: delta}

	out := waitOutput(t, g.Output())
	if out.Name != "out" {
		t.Fatalf("output name = %q, want out", out.Name)
	}
	if !out.Data.Equal(delta) {
		t.Fatalf("output payload mismatch")
	}

	close(g.Input())
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for graph shutdown")
	}
}

func TestGraphAllowsMultipleProcessorsWritingSameOutput(t *testing.T) {
	t.Parallel()

	p1 := newTestProcessor(func(in runtime.Input) []runtime.Output {
		return []runtime.Output{{Name: "out", Data: in.Data}}
	})
	p2 := newTestProcessor(func(in runtime.Input) []runtime.Output {
		return []runtime.Output{{Name: "out", Data: in.Data}}
	})

	g, err := runtime.NewGraph(runtime.GraphConfig{
		Processors: map[string]runtime.Processor{"p1": p1, "p2": p2},
		InputBindings: map[string][]runtime.Port{
			"in": {
				{Processor: "p1", Name: "in"},
				{Processor: "p2", Name: "in"},
			},
		},
		OutputBindings: map[string][]runtime.Port{
			"shared": {
				{Processor: "p1", Name: "out"},
				{Processor: "p2", Name: "out"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewGraph() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.Start(ctx)
	}()

	delta := zset.New()
	delta.Insert(testutils.StringElem("w"), 1)
	g.Input() <- runtime.Input{Name: "in", Data: delta}

	outs := collectOutputs(t, g.Output(), 2)
	for _, out := range outs {
		if out.Name != "shared" {
			t.Fatalf("output name = %q, want shared", out.Name)
		}
		if !out.Data.Equal(delta) {
			t.Fatalf("shared output payload mismatch")
		}
	}

	close(g.Input())
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for graph shutdown")
	}
}

func TestGraphRejectsUnboundInput(t *testing.T) {
	t.Parallel()

	p := newTestProcessor(func(in runtime.Input) []runtime.Output {
		return []runtime.Output{{Name: "out", Data: in.Data}}
	})

	g, err := runtime.NewGraph(runtime.GraphConfig{
		Processors: map[string]runtime.Processor{"p": p},
		InputBindings: map[string][]runtime.Port{
			"known": {{Processor: "p", Name: "in"}},
		},
	})
	if err != nil {
		t.Fatalf("NewGraph() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.Start(ctx)
	}()

	g.Input() <- runtime.Input{Name: "unknown", Data: zset.New()}

	select {
	case err := <-errCh:
		if !errors.Is(err, runtime.ErrGraphUnboundInput) {
			t.Fatalf("Start() error = %v, want ErrGraphUnboundInput", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for unbound input error")
	}
}

func collectOutputs(t *testing.T, ch <-chan runtime.Output, n int) []runtime.Output {
	t.Helper()
	outs := make([]runtime.Output, 0, n)
	for len(outs) < n {
		select {
		case out := <-ch:
			outs = append(outs, out)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %d outputs", n)
		}
	}
	return outs
}

func waitOutput(t *testing.T, ch <-chan runtime.Output) runtime.Output {
	t.Helper()
	select {
	case out := <-ch:
		return out
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for output")
		return runtime.Output{}
	}
}

type testProcessor struct {
	in  chan runtime.Input
	out chan runtime.Output
	f   func(runtime.Input) []runtime.Output
}

func newTestProcessor(f func(runtime.Input) []runtime.Output) *testProcessor {
	return &testProcessor{
		in:  make(chan runtime.Input, 8),
		out: make(chan runtime.Output, 8),
		f:   f,
	}
}

func (p *testProcessor) Input() chan<- runtime.Input   { return p.in }
func (p *testProcessor) Output() <-chan runtime.Output { return p.out }

func (p *testProcessor) Start(ctx context.Context) error {
	defer close(p.out)
	for {
		select {
		case <-ctx.Done():
			return nil
		case in, ok := <-p.in:
			if !ok {
				return nil
			}
			for _, out := range p.f(in) {
				select {
				case <-ctx.Done():
					return nil
				case p.out <- out:
				}
			}
		}
	}
}
