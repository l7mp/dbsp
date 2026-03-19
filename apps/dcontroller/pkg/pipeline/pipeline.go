// Package pipeline implements declarative data processing pipelines that transform
// Kubernetes resources using DBSP-based incremental computation.
//
// Pipelines define how source Kubernetes resources are joined, filtered, and
// aggregated to produce target view objects. They support complex relational
// operations while maintaining incremental update semantics for efficiency.
//
// Pipeline operations:
//   - @join: Combine multiple resource types with boolean conditions (must be first if present).
//   - @select: Filter objects based on boolean expressions.
//   - @project: Transform object structure and extract fields.
//   - @unwind: Expand array fields into multiple objects.
//   - @gather: Collect multiple objects into aggregated results.
//
// Example usage:
//
//	pipeline, _ := pipeline.New("my-op", target, sources,
//	    opv1a1.Pipeline{
//	        Expressions: []expression.Expression{
//	            {Op: "@join", Args: ...},
//	            {Op: "@select", Args: ...},
//	        },
//	    }, logger)
package pipeline

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"

	aggcompiler "github.com/l7mp/dbsp/dbsp/compiler/aggregation"
	"github.com/l7mp/dbsp/dbsp/transform"
	opv1a1 "github.com/l7mp/dcontroller/pkg/api/operator/v1alpha1"
	"github.com/l7mp/connectors/runtime/cache"
	"github.com/l7mp/connectors/runtime/object"
)

var _ Evaluator = &Pipeline{}

var ObjectKey = toolscache.MetaObjectToName

// Evaluator is a query that knows how to evaluate itself on a given delta and how to print itself.
type Evaluator interface {
	Evaluate(object.Delta) ([]object.Delta, error)
	Sync() ([]object.Delta, error)
	fmt.Stringer
	// GetTargetCache returns the pipeline's internal target cache (primarily for testing).
	GetTargetCache() *cache.Store
	// GetSourceCache returns the pipeline's internal source cache for a given GVK (primarily for testing).
	GetSourceCache(schema.GroupVersionKind) *cache.Store
}

// Pipeline is query that knows how to evaluate itself.
// Pipeline is not reentrant: Evaluate() and Sync() must not be called concurrently.
type Pipeline struct {
	operator string
	config   opv1a1.Pipeline
	sources  []schema.GroupVersionKind
	targets  []schema.GroupVersionKind

	sourceCache map[schema.GroupVersionKind]*cache.Store
	targetCache *cache.Store

	compiledCircuitName      string
	incrementalCircuitName   string
	compiledInputNodeMap     map[string]string
	compiledOutputNodeMap    map[string]string
	incrementalizedOutputMap map[string]string

	mu  sync.Mutex
	log logr.Logger
}

// New creates a new pipeline from the set of base objects and a seralized pipeline that writes
// into a given target.
func New(operator string, targets []schema.GroupVersionKind, sources []schema.GroupVersionKind, config opv1a1.Pipeline, log logr.Logger) (Evaluator, error) {
	p := &Pipeline{
		operator:    operator,
		config:      config,
		sources:     append([]schema.GroupVersionKind(nil), sources...),
		targets:     append([]schema.GroupVersionKind(nil), targets...),
		sourceCache: make(map[schema.GroupVersionKind]*cache.Store),
		targetCache: cache.NewStore(),
		log:         log,
	}

	sourceNames := make([]string, 0, len(sources))
	for _, src := range sources {
		sourceNames = append(sourceNames, src.Kind)
	}

	outputNames := make([]string, 0, len(targets))
	for _, target := range targets {
		outputNames = append(outputNames, target.Kind)
	}
	if len(outputNames) == 0 {
		outputNames = []string{"output"}
	}

	b, err := json.Marshal(config)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to serialize pipeline: %w", err))
	}

	compiler := aggcompiler.New(sourceNames, outputNames)
	ir, err := compiler.Parse(b)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to parse pipeline: %w", err))
	}

	query, err := compiler.Compile(ir)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to compile pipeline: %w", err))
	}

	incremental, err := transform.Incrementalize(query.Circuit)
	if err != nil {
		return nil, NewPipelineError(fmt.Errorf("failed to incrementalize circuit: %w", err))
	}

	p.compiledCircuitName = query.Circuit.Name()
	p.incrementalCircuitName = incremental.Name()
	p.compiledInputNodeMap = query.InputMap
	p.compiledOutputNodeMap = query.OutputMap
	p.incrementalizedOutputMap = query.OutputMap

	p.log.V(2).Info("pipeline shim ready",
		"compiled-circuit", p.compiledCircuitName,
		"incremental-circuit", p.incrementalCircuitName,
		"inputs", sourceNames,
		"outputs", outputNames)

	return p, nil
}

// String stringifies a pipeline.
func (p *Pipeline) String() string {
	return fmt.Sprintf("pipeline-shim(compiled=%s, incremental=%s)", p.compiledCircuitName, p.incrementalCircuitName)
}

// GetTargetCache returns the pipeline's internal target cache.
// This is primarily useful for testing to synchronize external state with the pipeline's view.
func (p *Pipeline) GetTargetCache() *cache.Store {
	return p.targetCache
}

// GetSourceCache returns the pipeline's internal source cache for a given GVK.
// This is primarily useful for testing to synchronize external state with the pipeline's view.
// Returns nil if no cache exists for the given GVK.
func (p *Pipeline) GetSourceCache(gvk schema.GroupVersionKind) *cache.Store {
	return p.sourceCache[gvk]
}

// Evaluate processes an pipeline on the given delta.
func (p *Pipeline) Evaluate(delta object.Delta) ([]object.Delta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.V(2).Info("pipeline shim evaluate", "event-type", delta.Type, "object", ObjectKey(delta.Object))
	return []object.Delta{}, nil
}

// Sync performs state-of-the-world reconciliation by computing the delta needed to bring the
// target state up to date with the current source state.
//
// This is used for periodic sources that need to re-render the entire target state periodically.
// On first call, it converts the incremental graph to a snapshot graph and caches both the graph
// and executor. On subsequent calls, it:
//  1. Converts source cache to ZSet (current input state)
//  2. Runs snapshot executor to compute required target state
//  3. Subtracts current target cache from required state to get delta
//  4. Applies delta to target cache to keep it synchronized
//  5. Returns delta as []object.Delta
func (p *Pipeline) Sync() ([]object.Delta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.V(2).Info("pipeline shim sync")
	return []object.Delta{}, nil
}
