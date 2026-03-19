package runtime

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/l7mp/dbsp/dbsp/zset"

	"golang.org/x/sync/errgroup"
)

var (
	// ErrGraphUnboundInput indicates that an external graph input has no bindings.
	ErrGraphUnboundInput = errors.New("runtime graph unbound input")
	// ErrGraphUnknownProcessor indicates that a graph port references an unknown processor.
	ErrGraphUnknownProcessor = errors.New("runtime graph unknown processor")
)

// Port identifies an input or output port on a processor.
type Port struct {
	Processor string
	Name      string
}

// GraphConfig configures a Graph runtime.
//
// InputBindings maps external input names to processor input ports.
// OutputBindings maps external output names to processor output ports.
// Links maps processor output ports to processor input ports.
type GraphConfig struct {
	Processors     map[string]Processor
	InputBindings  map[string][]Port
	OutputBindings map[string][]Port
	Links          map[Port][]Port
}

// Graph is a runtime that wires multiple processors into one input/output API.
type Graph struct {
	processors map[string]Processor

	inputBindings  map[string][]Port
	outputBindings map[Port][]string
	links          map[Port][]Port

	in  chan Input
	out chan Output
}

var _ Processor = (*Graph)(nil)

// NewGraph creates a new runtime graph.
func NewGraph(cfg GraphConfig) (*Graph, error) {
	processors := maps.Clone(cfg.Processors)
	if len(processors) == 0 {
		return nil, fmt.Errorf("runtime graph: at least one processor is required")
	}

	inputBindings := cloneInputBindings(cfg.InputBindings)
	outputBindings := normalizeOutputBindings(cfg.OutputBindings)
	links := cloneLinks(cfg.Links)

	for _, ports := range inputBindings {
		if err := validatePorts(ports, processors); err != nil {
			return nil, err
		}
	}
	for source, dests := range links {
		if err := validatePort(source, processors); err != nil {
			return nil, err
		}
		if err := validatePorts(dests, processors); err != nil {
			return nil, err
		}
	}
	for source := range outputBindings {
		if err := validatePort(source, processors); err != nil {
			return nil, err
		}
	}

	return &Graph{
		processors:     processors,
		inputBindings:  inputBindings,
		outputBindings: outputBindings,
		links:          links,
		in:             make(chan Input),
		out:            make(chan Output, DefaultOutputBufferSize),
	}, nil
}

// Input returns the runtime graph input channel.
func (g *Graph) Input() chan<- Input {
	return g.in
}

// Output returns the runtime graph output channel.
func (g *Graph) Output() <-chan Output {
	return g.out
}

// Start runs all processors and routing loops.
func (g *Graph) Start(ctx context.Context) error {
	gctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, egctx := errgroup.WithContext(gctx)

	for id, proc := range g.processors {
		id, proc := id, proc
		eg.Go(func() error {
			err := proc.Start(egctx)
			if err != nil {
				if isCancellation(err) && egctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("processor %q: %w", id, err)
			}
			if egctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("processor %q: %w", id, ErrRunnableReturnedNil)
		})

		eg.Go(func() error {
			for {
				select {
				case <-egctx.Done():
					return nil
				case out, ok := <-proc.Output():
					if !ok {
						return nil
					}
					if err := g.routeProcessorOutput(egctx, Port{Processor: id, Name: out.Name}, out.Data); err != nil {
						return err
					}
				}
			}
		})
	}

	eg.Go(func() error {
		for {
			select {
			case <-egctx.Done():
				return nil
			case in, ok := <-g.in:
				if !ok {
					cancel()
					return nil
				}
				if err := g.routeGraphInput(egctx, in); err != nil {
					return err
				}
			}
		}
	})

	err := eg.Wait()
	close(g.out)
	return err
}

func (g *Graph) routeGraphInput(ctx context.Context, in Input) error {
	dests := g.inputBindings[in.Name]
	if len(dests) == 0 {
		return fmt.Errorf("%w: %s", ErrGraphUnboundInput, in.Name)
	}

	for i, dest := range dests {
		data := in.Data
		if i < len(dests)-1 {
			data = in.Data.Clone()
		}
		if err := g.sendInput(ctx, dest, Input{Name: dest.Name, Data: data}); err != nil {
			return err
		}
	}

	return nil
}

func (g *Graph) routeProcessorOutput(ctx context.Context, source Port, data zset.ZSet) error {
	dests := g.links[source]
	for i, dest := range dests {
		payload := data
		if i < len(dests)-1 || len(g.outputBindings[source]) > 0 {
			payload = data.Clone()
		}
		if err := g.sendInput(ctx, dest, Input{Name: dest.Name, Data: payload}); err != nil {
			return err
		}
	}

	extOut := g.outputBindings[source]
	for i, outName := range extOut {
		payload := data
		if i < len(extOut)-1 {
			payload = data.Clone()
		}
		if err := g.emit(ctx, Output{Name: outName, Data: payload}); err != nil {
			return err
		}
	}

	return nil
}

func (g *Graph) sendInput(ctx context.Context, dest Port, in Input) error {
	proc := g.processors[dest.Processor]
	select {
	case <-ctx.Done():
		return nil
	case proc.Input() <- in:
		return nil
	}
}

func (g *Graph) emit(ctx context.Context, out Output) error {
	select {
	case <-ctx.Done():
		return nil
	case g.out <- out:
		return nil
	}
}

func normalizeOutputBindings(output map[string][]Port) map[Port][]string {
	normalized := map[Port][]string{}
	for outName, sources := range output {
		for _, source := range sources {
			normalized[source] = append(normalized[source], outName)
		}
	}
	return normalized
}

func cloneInputBindings(in map[string][]Port) map[string][]Port {
	if in == nil {
		return map[string][]Port{}
	}
	out := make(map[string][]Port, len(in))
	for name, ports := range in {
		out[name] = append([]Port(nil), ports...)
	}
	return out
}

func cloneLinks(in map[Port][]Port) map[Port][]Port {
	if in == nil {
		return map[Port][]Port{}
	}
	out := make(map[Port][]Port, len(in))
	for source, dests := range in {
		out[source] = append([]Port(nil), dests...)
	}
	return out
}

func validatePorts(ports []Port, processors map[string]Processor) error {
	for _, p := range ports {
		if err := validatePort(p, processors); err != nil {
			return err
		}
	}
	return nil
}

func validatePort(p Port, processors map[string]Processor) error {
	if _, ok := processors[p.Processor]; !ok {
		return fmt.Errorf("%w: %s", ErrGraphUnknownProcessor, p.Processor)
	}
	if p.Name == "" {
		return fmt.Errorf("runtime graph: port name is required")
	}
	return nil
}
