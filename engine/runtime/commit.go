package runtime

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/transform"
)

// CommitOptions configure the installation of one circuit.
type CommitOptions struct {
	// AdaptInputs and AdaptOutputs name the logical inputs and outputs
	// that receive snapshot adapters (an integrator on an input, a
	// differentiator on an output). nil selects the default: every
	// boundary for a compiled circuit, none for a hand-built one. An
	// explicit list (empty included) is honored literally: partially
	// adapted circuits are legitimate boundary configurations owned by
	// the caller.
	AdaptInputs  *[]string
	AdaptOutputs *[]string
	// Compiled marks a circuit compiled from a pipeline or SQL program.
	// Compiled circuits are born snapshot and default to full
	// adaptation; hand-built graph circuits default to none.
	Compiled bool
	// Replace, when set, is the previously installed processor this
	// commit replaces.
	Replace *Circuit
	// DryRun builds and validates but installs nothing.
	DryRun bool
	// Logger is the circuit's logger; the runtime's own when unset.
	Logger logr.Logger
}

// CommitCircuit is the single install point for every circuit, loader- and
// script-built alike: it applies the transform chain in canonical order,
// places the snapshot adapters, validates, and installs the processor.
//
// Every committed circuit computes the same operator, Q^Δ = D ∘ Q ∘ ∫. The
// snapshot adapters are its definitional, non-incremental evaluation:
// recompute over the integrated state, differentiate the result. The
// Incrementalizer is the optimizer that rewrites the composition into the
// efficient incremental form, consuming the boundary adapters in the
// process - which is why a chain that incrementalizes defaults to no
// adapters.
func (rt *Runtime) CommitCircuit(name string, q *compiler.Query, specs []transform.TransformSpec, opts CommitOptions) (*Circuit, error) {
	if name == "" {
		name = q.Circuit.Name()
	}

	incremental := false
	for _, ts := range specs {
		if ts.Name == string(transform.Incrementalizer) {
			incremental = true
		}
	}

	c := q.Circuit.Clone()
	c.SetName(name)
	if err := ValidateCircuit(c); err != nil {
		return nil, err
	}

	if len(specs) > 0 {
		chain, err := transform.NewChainFromSpecs(specs)
		if err != nil {
			return nil, fmt.Errorf("transform: %w", err)
		}
		c, err = chain.Transform(c)
		if err != nil {
			return nil, fmt.Errorf("transform: %w", err)
		}
		if err := ValidateCircuit(c); err != nil {
			return nil, fmt.Errorf("transform: %w", err)
		}
	}

	adaptAll := opts.Compiled && !incremental
	inputs, err := resolveAdapt(opts.AdaptInputs, adaptAll, q.InputMap, "input")
	if err != nil {
		return nil, err
	}
	outputs, err := resolveAdapt(opts.AdaptOutputs, adaptAll, q.OutputMap, "output")
	if err != nil {
		return nil, err
	}
	for _, logical := range inputs {
		if err := adaptInput(c, q.InputMap[logical], logical); err != nil {
			return nil, err
		}
	}
	for _, logical := range outputs {
		if err := adaptOutput(c, q.OutputMap[logical], logical); err != nil {
			return nil, err
		}
	}
	if len(inputs) > 0 || len(outputs) > 0 {
		if err := ValidateCircuit(c); err != nil {
			return nil, err
		}
	}

	if opts.DryRun {
		return nil, nil
	}

	query := *q
	query.Circuit = c

	// A transform may inject input nodes of its own (the DualRateSmith's
	// tick input): subscribe them like compiled inputs, keyed by the topic
	// stem of the node ID.
	inputMap := make(map[string]string, len(q.InputMap))
	compiled := make(map[string]bool, len(q.InputMap))
	for k, v := range q.InputMap {
		inputMap[k] = v
		compiled[v] = true
	}
	for _, n := range c.Inputs() {
		if !compiled[n.ID] {
			inputMap[strings.TrimPrefix(n.ID, "input_")] = n.ID
		}
	}
	query.InputMap = inputMap

	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = rt.Logger()
	}
	proc, err := NewCircuit(name, rt, &query, logger)
	if err != nil {
		return nil, fmt.Errorf("runtime circuit: %w", err)
	}
	if opts.Replace != nil {
		rt.Stop(opts.Replace)
	}
	if err := rt.Add(proc); err != nil {
		return nil, fmt.Errorf("runtime add circuit: %w", err)
	}
	return proc, nil
}

// resolveAdapt turns the adapter selection into the list of logical
// boundary names: nil selects every boundary when all is set (and none
// otherwise), an explicit list is checked against the boundary map and
// honored literally.
func resolveAdapt(sel *[]string, all bool, bounds map[string]string, side string) ([]string, error) {
	if sel == nil {
		if !all {
			return nil, nil
		}
		return sortedKeys(bounds), nil
	}
	for _, logical := range *sel {
		if _, ok := bounds[logical]; !ok {
			return nil, fmt.Errorf("commit: unknown %s %q", side, logical)
		}
	}
	return *sel, nil
}

// adaptInput places the snapshot adapter on one input: an integrator
// holding the stream's running state, so the circuit body computes over
// full state.
func adaptInput(c *circuit.Circuit, nodeID, logical string) error {
	id := "input-" + logical + "-adaptor"
	if err := c.AddNode(circuit.Integrate(id)); err != nil {
		return err
	}
	for _, e := range append([]*circuit.Edge(nil), c.EdgesFrom(nodeID)...) {
		if err := c.RemoveEdge(e.From, e.To, e.Port); err != nil {
			return err
		}
		if err := c.AddEdge(circuit.NewEdge(id, e.To, e.Port)); err != nil {
			return err
		}
	}
	return c.AddEdge(circuit.NewEdge(nodeID, id, 0))
}

// adaptOutput places the snapshot adapter on one output: a
// differentiator emitting the change of the recomputed result, deletions
// included.
func adaptOutput(c *circuit.Circuit, nodeID, logical string) error {
	id := "output-" + logical + "-adaptor"
	if err := c.AddNode(circuit.Differentiate(id)); err != nil {
		return err
	}
	for _, e := range append([]*circuit.Edge(nil), c.EdgesTo(nodeID)...) {
		if err := c.RemoveEdge(e.From, e.To, e.Port); err != nil {
			return err
		}
		if err := c.AddEdge(circuit.NewEdge(e.From, id, e.Port)); err != nil {
			return err
		}
	}
	return c.AddEdge(circuit.NewEdge(id, nodeID, 0))
}

// ValidateCircuit runs the one well-formedness validator and folds the
// errors into one.
func ValidateCircuit(c *circuit.Circuit) error {
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
