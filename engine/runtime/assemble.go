package runtime

import (
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/engine/circuit"
	aggcompiler "github.com/l7mp/dbsp/engine/compiler/aggregation"
	"github.com/l7mp/dbsp/engine/spec"
	"github.com/l7mp/dbsp/engine/transform"
)

// Assemble populates the runtime from its serialized spec: every
// circuit's program is compiled and transformed, circuits and target
// consumers are constructed with their subscriptions in place, and
// sources are built but not producing. Nothing flows until the runtime
// runs (Start). The runtime must be named: the name keys the runtime's own
// view group and its connector services.
//
// Each stream materializes as one topic named by the stream. reg routes
// the bindings; nil means no connectors, so a spec with bindings needs a
// registry while a bindings-free one assembles without.
func (rt *Runtime) Assemble(op *spec.RuntimeSpec, reg *ConnectorRegistry) error {
	if err := op.Validate(); err != nil {
		return err
	}
	if rt.Name() == "" {
		return fmt.Errorf("the runtime must be named")
	}
	if reg == nil {
		reg = NewConnectorRegistry()
	}

	// The source and target streams, in declaration order. A stream fed
	// by a source and consumed by a target would short-circuit the
	// binding pair around every circuit, so the collision is refused.
	srcStreams := []string{}
	srcSeen := map[string]bool{}
	for i := range op.Sources {
		stream := streamName(op.Sources[i].As, op.Sources[i].Kind)
		if !srcSeen[stream] {
			srcSeen[stream] = true
			srcStreams = append(srcStreams, stream)
		}
	}
	tgtStreams := []string{}
	tgtSeen := map[string]bool{}
	for i := range op.Targets {
		stream := streamName(op.Targets[i].As, op.Targets[i].Kind)
		if srcSeen[stream] {
			return fmt.Errorf("stream %q is fed by a source and consumed by a target", stream)
		}
		if !tgtSeen[stream] {
			tgtSeen[stream] = true
			tgtStreams = append(tgtStreams, stream)
		}
	}

	// Init every referenced factory once, before any binding is built.
	inited := map[string]bool{}
	resources := make([]spec.Resource, 0, len(op.Sources)+len(op.Targets))
	for _, s := range op.Sources {
		resources = append(resources, s.Resource)
	}
	for _, t := range op.Targets {
		resources = append(resources, t.Resource)
	}
	for _, res := range resources {
		f, err := reg.factoryFor(res)
		if err != nil {
			return err
		}
		if inited[f.Name] {
			continue
		}
		inited[f.Name] = true
		if f.Init != nil {
			if err := f.Init(rt, op); err != nil {
				return fmt.Errorf("connector %q: %w", f.Name, err)
			}
		}
	}

	// The circuits. Inputs and outputs default to the single source and
	// target stream when unambiguous.
	consumed := map[string]bool{}
	produced := map[string]bool{}
	for ci := range op.Circuits {
		c := &op.Circuits[ci]
		inputs := c.Inputs
		if len(inputs) == 0 {
			if len(srcStreams) != 1 {
				return fmt.Errorf("circuit %q: no inputs and %d source streams to default to", c.Name, len(srcStreams))
			}
			inputs = srcStreams
		}
		outputs := c.Outputs
		if len(outputs) == 0 {
			if len(tgtStreams) != 1 {
				return fmt.Errorf("circuit %q: no outputs and %d target streams to default to", c.Name, len(tgtStreams))
			}
			outputs = tgtStreams
		}
		for _, s := range inputs {
			consumed[s] = true
		}
		for _, s := range outputs {
			produced[s] = true
		}
		if err := rt.assembleCircuit(c, inputs, outputs); err != nil {
			return fmt.Errorf("circuit %q: %w", c.Name, err)
		}
	}
	for _, stream := range srcStreams {
		if !consumed[stream] {
			return fmt.Errorf("source stream %q feeds no circuit", stream)
		}
	}
	for _, stream := range tgtStreams {
		if !produced[stream] {
			return fmt.Errorf("target stream %q is produced by no circuit", stream)
		}
	}

	// The bindings. The runtime is idle, so construction order is
	// irrelevant: subscriptions are taken at construction and nothing
	// produces before the runtime starts.
	for i := range op.Sources {
		s := &op.Sources[i]
		f, err := reg.factoryFor(s.Resource)
		if err != nil {
			return err
		}
		if f.NewSource == nil {
			return fmt.Errorf("source %q: connector %q offers no sources", s.Kind, f.Name)
		}
		r, err := f.NewSource(rt, s, streamName(s.As, s.Kind))
		if err != nil {
			return fmt.Errorf("source %q: %w", s.Kind, err)
		}
		if err := rt.Add(r); err != nil {
			return err
		}
	}
	for i := range op.Targets {
		t := &op.Targets[i]
		f, err := reg.factoryFor(t.Resource)
		if err != nil {
			return err
		}
		if f.NewTarget == nil {
			return fmt.Errorf("target %q: connector %q offers no targets", t.Kind, f.Name)
		}
		r, err := f.NewTarget(rt, t, streamName(t.As, t.Kind))
		if err != nil {
			return fmt.Errorf("target %q: %w", t.Kind, err)
		}
		if err := rt.Add(r); err != nil {
			return err
		}
	}
	return nil
}

// jacketCircuit wraps a compiled snapshot program for the delta bus:
// every input feeds through an integrator and every output through a
// differentiator, turning Q into ∫ -> Q -> D.
func jacketCircuit(c *circuit.Circuit) error {
	for _, in := range c.Inputs() {
		intID := in.ID + "-sotw-integrate"
		if err := c.AddNode(circuit.Integrate(intID)); err != nil {
			return err
		}
		for _, e := range append([]*circuit.Edge(nil), c.EdgesFrom(in.ID)...) {
			if err := c.RemoveEdge(e.From, e.To, e.Port); err != nil {
				return err
			}
			if err := c.AddEdge(circuit.NewEdge(intID, e.To, e.Port)); err != nil {
				return err
			}
		}
		if err := c.AddEdge(circuit.NewEdge(in.ID, intID, 0)); err != nil {
			return err
		}
	}
	for _, out := range c.Outputs() {
		diffID := out.ID + "-sotw-differentiate"
		if err := c.AddNode(circuit.Differentiate(diffID)); err != nil {
			return err
		}
		for _, e := range append([]*circuit.Edge(nil), c.EdgesTo(out.ID)...) {
			if err := c.RemoveEdge(e.From, e.To, e.Port); err != nil {
				return err
			}
			if err := c.AddEdge(circuit.NewEdge(e.From, diffID, e.Port)); err != nil {
				return err
			}
		}
		if err := c.AddEdge(circuit.NewEdge(diffID, out.ID, 0)); err != nil {
			return err
		}
	}
	return nil
}

// streamName is the stream a binding feeds or consumes: the explicit
// `as`, defaulting to the resource kind.
func streamName(as, kind string) string {
	if s := strings.TrimSpace(as); s != "" {
		return s
	}
	return kind
}

func validateCircuit(c *circuit.Circuit) error {
	errs := c.Validate()
	if len(errs) == 0 {
		return nil
	}
	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		messages = append(messages, err.Error())
	}
	return fmt.Errorf("circuit validation failed: %s", strings.Join(messages, "; "))
}

// assembleCircuit compiles one circuit over its streams, applies its
// transform chain, and adds it to the runtime.
func (rt *Runtime) assembleCircuit(c *spec.CircuitSpec, inputs, outputs []string) error {
	if c.Pipeline == nil {
		return fmt.Errorf("only pipeline programs are supported")
	}

	ins := make([]aggcompiler.Binding, 0, len(inputs))
	for _, s := range inputs {
		ins = append(ins, aggcompiler.Binding{Name: s, Logical: s})
	}
	outs := make([]aggcompiler.Binding, 0, len(outputs))
	for _, s := range outputs {
		outs = append(outs, aggcompiler.Binding{Name: s, Logical: s})
	}

	compiled, err := aggcompiler.New(ins, outs).CompileString(string(*c.Pipeline))
	if err != nil {
		return err
	}
	compiled.Circuit.SetName(c.Name)
	if err := validateCircuit(compiled.Circuit); err != nil {
		return err
	}

	// A chain without the Incrementalizer asks for snapshot execution:
	// the program runs non-incrementally on the delta bus as ∫ -> Q -> D.
	// The input integrators hold the full current state (a silent input's
	// integral simply stands, so multi-input steps are always complete),
	// and the output differentiation emits the change of the recomputed
	// result, deletions included. With the Incrementalizer the program
	// compiles to Q^Δ instead; the two are the same semantics by the DBSP
	// equation Q^Δ = D ∘ Q ∘ ∫.
	incremental := false
	for _, t := range c.Transforms {
		if t.Name == "Incrementalizer" {
			incremental = true
		}
	}
	if !incremental {
		if err := jacketCircuit(compiled.Circuit); err != nil {
			return err
		}
		if err := validateCircuit(compiled.Circuit); err != nil {
			return err
		}
	}

	transformed := compiled.Circuit
	if len(c.Transforms) > 0 {
		chain, err := transform.NewChainFromSpecs(c.Transforms)
		if err != nil {
			return fmt.Errorf("transform: %w", err)
		}
		transformed, err = chain.Transform(compiled.Circuit)
		if err != nil {
			return fmt.Errorf("transform: %w", err)
		}
		if err := validateCircuit(transformed); err != nil {
			return fmt.Errorf("transform: %w", err)
		}
	}

	query := *compiled
	query.Circuit = transformed
	proc, err := NewCircuit(c.Name, rt, &query, rt.Logger())
	if err != nil {
		return fmt.Errorf("runtime circuit: %w", err)
	}
	return rt.Add(proc)
}
