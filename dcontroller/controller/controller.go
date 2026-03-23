package controller

import (
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	"github.com/l7mp/dbsp/connectors/misc"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	"github.com/l7mp/dbsp/engine/compiler"
	aggcompiler "github.com/l7mp/dbsp/engine/compiler/aggregation"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/transform"
)

// QueryTransformer rewrites a compiled query before the circuit is instantiated.
// It may change the circuit structure, input map, or output map.
type QueryTransformer func(*compiler.Query) (*compiler.Query, error)

// Config holds the configuration for a Controller.
type Config struct {
	// OperatorName is the owning operator name used for resolving default view API groups.
	// If empty, the controller name is used for backward compatibility.
	OperatorName string

	// Spec is the declarative controller specification.
	Spec opv1a1.Controller

	// Runtime is the shared DBSP pub/sub and lifecycle manager.
	Runtime *dbspruntime.Runtime

	// K8sRuntime is the Kubernetes runtime providing the composite client and REST mapper.
	// Required for Watcher sources and Kubernetes consumers; may be nil if only virtual
	// sources (Periodic, OneShot) are used without Kubernetes consumers.
	K8sRuntime *k8sruntime.Runtime

	// Transformer is an optional function that rewrites the compiled query
	// before the circuit is instantiated.
	Transformer QueryTransformer

	// Logger is used for structured logging.
	Logger logr.Logger
}

// Controller wires producers, a DBSP circuit, and consumers for a single declarative pipeline.
type Controller struct {
	cfg     Config
	circuit *dbspruntime.Circuit
	mapper  meta.RESTMapper
	log     logr.Logger
}

// New creates and wires a Controller from the given Config.
//
// The sequence is: compile the pipeline spec, incrementalize the circuit, optionally
// transform it, then register producers and consumers in the runtime.
func New(cfg Config) (*Controller, error) {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	operatorName := cfg.OperatorName
	if operatorName == "" {
		operatorName = cfg.Spec.Name
	}

	// 1. Resolve logical topic names from sources and targets.
	sourceKinds := make([]string, 0, len(cfg.Spec.Sources))
	for _, s := range cfg.Spec.Sources {
		sourceKinds = append(sourceKinds, s.Kind)
	}
	targetKinds := make([]string, 0, len(cfg.Spec.Targets))
	for _, t := range cfg.Spec.Targets {
		targetKinds = append(targetKinds, t.Kind)
	}

	// 2. Compile the circuit spec.
	// Enforce that exactly one of spec.Pipeline, spec.SQL or spec.Circuit is given.
	if !exactlyOneOf3(cfg.Spec.Pipeline != nil, cfg.Spec.SQL != nil, cfg.Spec.Circuit != nil) {
		return nil, fmt.Errorf("exactly one of spec.Pipeline, spec.SQL or spec.Circuit must be set in %v",
			cfg.Spec)
	}

	var q *compiler.Query
	switch {
	case cfg.Spec.Pipeline != nil:
		comp := aggcompiler.New(sourceKinds, targetKinds)
		ir, err := comp.Parse(cfg.Spec.Pipeline.Raw)
		if err != nil {
			return nil, fmt.Errorf("controller: failed to parse pipeline: %w", err)
		}
		q, err = comp.Compile(ir)
		if err != nil {
			return nil, fmt.Errorf("controller: failed to compile pipeline: %w", err)
		}

	default:
		return nil, fmt.Errorf("unimplemented: %v", cfg.Spec)
	}

	// 3. Validate the circuit.
	if cfg.Spec.Type == opv1a1.ControllerTypeIncremental {
		if err := errors.Join(q.Circuit.Validate()...); err != nil {
			return nil, fmt.Errorf("controller: failed to validate circuit: %w", err)
		}
	}

	// 4. Incrementalize the circuit.
	var err error
	q.Circuit, err = transform.Incrementalize(q.Circuit)
	if err != nil {
		return nil, fmt.Errorf("controller: failed to incrementalize circuit: %w", err)
	}

	// 5. Apply optional query transformer.
	if cfg.Transformer != nil {
		q, err = cfg.Transformer(q)
		if err != nil {
			return nil, fmt.Errorf("controller: transformer failed: %w", err)
		}
	}

	// 6. Instantiate the circuit in the runtime.
	circuit, err := dbspruntime.NewCircuit(cfg.Spec.Name, cfg.Runtime, q, log)
	if err != nil {
		return nil, fmt.Errorf("controller: failed to create runtime circuit: %w", err)
	}

	ctrl := &Controller{
		cfg:     cfg,
		circuit: circuit,
		log:     log,
	}
	if cfg.K8sRuntime != nil {
		ctrl.mapper = cfg.K8sRuntime.GetRESTMapper()
	}

	// 7. Wire one producer per source.
	for _, src := range cfg.Spec.Sources {
		if err := ctrl.addProducer(operatorName, src); err != nil {
			return nil, err
		}
	}

	// 8. Wire one consumer per target.
	for _, tgt := range cfg.Spec.Targets {
		if err := ctrl.addConsumer(operatorName, tgt); err != nil {
			return nil, err
		}
	}

	// 9. Add circuit to runtime (after producers/consumers so it is started last).
	if err := cfg.Runtime.Add(circuit); err != nil {
		return nil, fmt.Errorf("controller: failed to register circuit: %w", err)
	}

	return ctrl, nil
}

// GetCircuit returns the runtime circuit (primarily for testing).
func (c *Controller) GetCircuit() *dbspruntime.Circuit {
	return c.circuit
}

// GetName returns the controller name from its spec.
func (c *Controller) GetName() string {
	return c.cfg.Spec.Name
}

// GetGVKs returns all source and target GVKs referenced by the controller.
func (c *Controller) GetGVKs() []schema.GroupVersionKind {
	ret := make([]schema.GroupVersionKind, 0, len(c.cfg.Spec.Sources)+len(c.cfg.Spec.Targets))
	operatorName := c.cfg.OperatorName
	if operatorName == "" {
		operatorName = c.cfg.Spec.Name
	}

	for _, src := range c.cfg.Spec.Sources {
		gvk, err := sourceGVK(operatorName, src, c.mapper)
		if err == nil {
			ret = append(ret, gvk)
		}
	}

	for _, tgt := range c.cfg.Spec.Targets {
		gvk, err := targetGVK(operatorName, tgt, c.mapper)
		if err == nil {
			ret = append(ret, gvk)
		}
	}

	return ret
}

// addProducer creates and registers a producer for a single source.
func (c *Controller) addProducer(operatorName string, src opv1a1.Source) error {
	gvk, err := sourceGVK(operatorName, src, c.mapper)
	if err != nil {
		return fmt.Errorf("controller: failed to resolve GVK for source %q: %w", src.Kind, err)
	}

	ns := ""
	if src.Namespace != nil {
		ns = *src.Namespace
	}

	switch src.Type {
	case opv1a1.Watcher, "":
		if c.cfg.K8sRuntime == nil {
			return fmt.Errorf("controller: kubernetes runtime is required for watcher source %q", src.Kind)
		}
		w, err := k8sproducer.NewWatcher(k8sproducer.Config{
			Client:        c.cfg.K8sRuntime.GetClient(),
			SourceGVK:     gvk,
			Name:          fmt.Sprintf("%s.source.%s.watcher", c.cfg.Spec.Name, src.Kind),
			InputName:     src.Kind,
			Namespace:     ns,
			LabelSelector: src.LabelSelector,
			Predicate:     src.Predicate,
			Runtime:       c.cfg.Runtime,
			Logger:        c.log,
		})
		if err != nil {
			return fmt.Errorf("controller: failed to create watcher for %q: %w", src.Kind, err)
		}
		if err := c.cfg.Runtime.Add(w); err != nil {
			return fmt.Errorf("controller: failed to register watcher for %q: %w", src.Kind, err)
		}

	case opv1a1.Periodic:
		period, err := parsePeriod(src)
		if err != nil {
			return fmt.Errorf("controller: invalid period for %q: %w", src.Kind, err)
		}
		if period <= 0 {
			return fmt.Errorf("controller: periodic source %q requires a positive period", src.Kind)
		}
		p, err := misc.NewPeriodicProducer(misc.PeriodicConfig{
			Name:        fmt.Sprintf("%s.source.%s.periodic", c.cfg.Spec.Name, src.Kind),
			InputName:   src.Kind,
			TriggerKind: gvk.Kind,
			Namespace:   ns,
			Period:      period,
			Runtime:     c.cfg.Runtime,
			Logger:      c.log,
		})
		if err != nil {
			return fmt.Errorf("controller: failed to create periodic producer for %q: %w", src.Kind, err)
		}
		if err := c.cfg.Runtime.Add(p); err != nil {
			return fmt.Errorf("controller: failed to register periodic producer for %q: %w", src.Kind, err)
		}

	case opv1a1.OneShot:
		p, err := misc.NewOneShotProducer(misc.OneShotConfig{
			Name:        fmt.Sprintf("%s.source.%s.oneshot", c.cfg.Spec.Name, src.Kind),
			InputName:   src.Kind,
			TriggerKind: gvk.Kind,
			Namespace:   ns,
			Runtime:     c.cfg.Runtime,
			Logger:      c.log,
		})
		if err != nil {
			return fmt.Errorf("controller: failed to create one-shot producer for %q: %w", src.Kind, err)
		}
		if err := c.cfg.Runtime.Add(p); err != nil {
			return fmt.Errorf("controller: failed to register one-shot producer for %q: %w", src.Kind, err)
		}

	default:
		return fmt.Errorf("controller: unknown source type %q for %q", src.Type, src.Kind)
	}

	return nil
}

// addConsumer creates and registers a consumer for a single target.
func (c *Controller) addConsumer(operatorName string, tgt opv1a1.Target) error {
	if c.cfg.K8sRuntime == nil {
		return fmt.Errorf("controller: kubernetes runtime is required for target %q", tgt.Kind)
	}

	gvk, err := targetGVK(operatorName, tgt, c.mapper)
	if err != nil {
		return fmt.Errorf("controller: failed to resolve GVK for target %q: %w", tgt.Kind, err)
	}

	targetType := tgt.Type
	if targetType == "" {
		targetType = opv1a1.Updater
	}

	cfg := k8sconsumer.Config{
		Client:     c.cfg.K8sRuntime.GetClient(),
		Name:       fmt.Sprintf("%s.target.%s.%s", c.cfg.Spec.Name, tgt.Kind, targetType),
		OutputName: tgt.Kind,
		TargetGVK:  gvk,
		Runtime:    c.cfg.Runtime,
		Logger:     c.log,
	}

	switch tgt.Type {
	case opv1a1.Updater, "":
		u, err := k8sconsumer.NewUpdater(cfg)
		if err != nil {
			return fmt.Errorf("controller: failed to create updater for %q: %w", tgt.Kind, err)
		}
		if err := c.cfg.Runtime.Add(u); err != nil {
			return fmt.Errorf("controller: failed to register updater for %q: %w", tgt.Kind, err)
		}

	case opv1a1.Patcher:
		p, err := k8sconsumer.NewPatcher(cfg)
		if err != nil {
			return fmt.Errorf("controller: failed to create patcher for %q: %w", tgt.Kind, err)
		}
		if err := c.cfg.Runtime.Add(p); err != nil {
			return fmt.Errorf("controller: failed to register patcher for %q: %w", tgt.Kind, err)
		}

	default:
		return fmt.Errorf("controller: unknown target type %q for %q", tgt.Type, tgt.Kind)
	}

	return nil
}

func exactlyOneOf3(a, b, c bool) bool {
	n := 0
	if a {
		n++
	}
	if b {
		n++
	}
	if c {
		n++
	}
	return n == 1
}
