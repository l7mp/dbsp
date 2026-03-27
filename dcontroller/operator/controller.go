package operator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	"github.com/l7mp/dbsp/engine/circuit"
	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

const (
	watcherComponentName   = "operator-controller.watcher"
	updaterComponentName   = "operator-controller.updater"
	processorComponentName = "operator-controller.processor"

	stopWaitTimeout = 5 * time.Second
)

var (
	operatorInputTopic  = circuit.InputTopic("operator-controller", "operator")
	operatorStatusTopic = circuit.OutputTopic("operator-controller", "operator-status")
)

var operatorGVK = opv1a1.GroupVersion.WithKind("Operator")

// OperatorController reconciles Operator CRs using DBSP runtime components.
//
// The controller wires:
//  1. a Kubernetes Watcher producer for Operator CRDs,
//  2. a processor that manages in-memory Operators and emits status updates,
//  3. a Kubernetes Updater consumer that writes Operator status.
type OperatorController struct {
	k8sRuntime  *k8sruntime.Runtime
	dbspRuntime *dbspruntime.Runtime
	processor   *operatorProcessor

	log logr.Logger
}

// NewOperatorController creates the full OperatorController runtime wiring.
func NewOperatorController(cfg k8sruntime.Config) (*OperatorController, error) {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("operator-controller")

	k8srt, err := k8sruntime.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes runtime: %w", err)
	}

	dbsprt := dbspruntime.NewRuntime(log.WithName("dbsp-runtime"))

	watcher, err := k8sproducer.NewWatcher(k8sproducer.Config{
		Name:      watcherComponentName,
		Client:    k8srt.GetClient(),
		SourceGVK: operatorGVK,
		InputName: operatorInputTopic,
		Runtime:   dbsprt,
		Logger:    log,
	})
	if err != nil {
		return nil, fmt.Errorf("create operator watcher: %w", err)
	}

	updater, err := k8sconsumer.NewUpdater(k8sconsumer.Config{
		Name:       updaterComponentName,
		Client:     k8srt.GetClient(),
		OutputName: operatorStatusTopic,
		TargetGVK:  operatorGVK,
		Runtime:    dbsprt,
		Logger:     log,
	})
	if err != nil {
		return nil, fmt.Errorf("create operator status updater: %w", err)
	}

	proc, err := newOperatorProcessor(processorConfig{
		Name:        processorComponentName,
		InputTopic:  operatorInputTopic,
		OutputTopic: operatorStatusTopic,
		Runtime:     dbsprt,
		K8sRuntime:  k8srt,
		Logger:      log,
	})
	if err != nil {
		return nil, fmt.Errorf("create operator processor: %w", err)
	}

	if err := dbsprt.Add(watcher); err != nil {
		return nil, fmt.Errorf("register watcher: %w", err)
	}
	if err := dbsprt.Add(proc); err != nil {
		return nil, fmt.Errorf("register processor: %w", err)
	}
	if err := dbsprt.Add(updater); err != nil {
		return nil, fmt.Errorf("register updater: %w", err)
	}

	return &OperatorController{
		k8sRuntime:  k8srt,
		dbspRuntime: dbsprt,
		processor:   proc,
		log:         log,
	}, nil
}

// Start runs the Kubernetes runtime and the DBSP runtime until ctx is cancelled.
func (c *OperatorController) Start(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return c.k8sRuntime.Start(gctx)
	})

	g.Go(func() error {
		return c.dbspRuntime.Start(gctx)
	})

	return g.Wait()
}

// GetClient returns the composite Kubernetes client used by the operator controller.
func (c *OperatorController) GetClient() client.Client {
	return c.k8sRuntime.GetClient()
}

type processorConfig struct {
	Name        string
	InputTopic  string
	OutputTopic string
	Runtime     *dbspruntime.Runtime
	K8sRuntime  *k8sruntime.Runtime
	Logger      logr.Logger
}

type managedOperator struct {
	op     *Operator
	cancel context.CancelFunc
	done   chan struct{}
}

// operatorProcessor is a DBSP runtime processor that translates Operator CR deltas
// into operator lifecycle operations and status update events.
type operatorProcessor struct {
	*dbspruntime.BaseProcessor

	outputTopic string
	k8srt       *k8sruntime.Runtime

	mu        sync.Mutex
	ctx       context.Context
	operators map[types.NamespacedName]*managedOperator

	log logr.Logger
}

var _ dbspruntime.Processor = (*operatorProcessor)(nil)

func newOperatorProcessor(cfg processorConfig) (*operatorProcessor, error) {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	sub := cfg.Runtime.NewSubscriber()
	base, err := dbspruntime.NewBaseProcessor(dbspruntime.BaseProcessorConfig{
		Name:          cfg.Name,
		Publisher:     cfg.Runtime.NewPublisher(),
		Subscriber:    sub,
		ErrorReporter: cfg.Runtime,
		Logger:        log.WithName("processor"),
		Topics:        []string{cfg.InputTopic},
	})
	if err != nil {
		return nil, err
	}

	return &operatorProcessor{
		BaseProcessor: base,
		outputTopic:   cfg.OutputTopic,
		k8srt:         cfg.K8sRuntime,
		operators:     map[types.NamespacedName]*managedOperator{},
		log:           log.WithName("processor"),
	}, nil
}

func (p *operatorProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	p.ctx = ctx
	p.mu.Unlock()

	defer p.stopAllOperators()

	return p.Run(ctx, p)
}

func (p *operatorProcessor) Consume(ctx context.Context, in dbspruntime.Event) error {
	type opAction struct {
		op     *opv1a1.Operator
		weight zset.Weight
	}

	actions := make([]opAction, 0, in.Data.Size())
	for _, entry := range in.Data.Entries() {
		op, err := decodeOperator(entry.Document)
		if err != nil {
			return fmt.Errorf("decode operator event: %w", err)
		}
		actions = append(actions, opAction{op: op, weight: entry.Weight})
	}

	for _, action := range actions {
		if action.weight < 0 {
			p.deleteOperator(client.ObjectKeyFromObject(action.op))
		}
	}

	for _, action := range actions {
		if action.weight > 0 {
			if err := p.upsertOperator(ctx, action.op); err != nil {
				status := failedOperatorStatus(action.op.GetGeneration(), err)
				if pubErr := p.publishStatus(action.op, status); pubErr != nil {
					return errorsJoin(err, fmt.Errorf("publish failed status: %w", pubErr))
				}
				return err
			}
		}
	}

	return nil
}

func (p *operatorProcessor) upsertOperator(ctx context.Context, spec *opv1a1.Operator) error {
	key := client.ObjectKeyFromObject(spec)

	p.deleteOperator(key)

	op, err := New(spec.GetName(), Config{
		Spec:       spec.Spec,
		K8sRuntime: p.k8srt,
		Logger:     p.log.WithValues("operator", spec.GetName()),
	})
	if err != nil {
		return fmt.Errorf("create operator %q: %w", spec.GetName(), err)
	}

	opCtx, cancel := context.WithCancel(ctx)
	entry := &managedOperator{op: op, cancel: cancel, done: make(chan struct{})}

	p.mu.Lock()
	p.operators[key] = entry
	p.mu.Unlock()

	go func() {
		defer close(entry.done)
		if startErr := op.Start(opCtx); startErr != nil && opCtx.Err() == nil {
			p.HandleError(fmt.Errorf("operator %q exited with error: %w", op.GetName(), startErr))
		}
	}()

	if err := p.publishStatus(spec, op.GetStatus(spec.GetGeneration())); err != nil {
		return fmt.Errorf("publish status for %q: %w", spec.GetName(), err)
	}

	return nil
}

func (p *operatorProcessor) deleteOperator(key types.NamespacedName) {
	p.mu.Lock()
	entry, ok := p.operators[key]
	if ok {
		delete(p.operators, key)
	}
	p.mu.Unlock()

	if !ok {
		return
	}

	entry.op.UnregisterGVKs()
	if entry.cancel != nil {
		entry.cancel()
	}

	if entry.done != nil {
		select {
		case <-entry.done:
		case <-time.After(stopWaitTimeout):
			p.log.V(1).Info("timeout while waiting for operator to stop", "operator", key.String())
		}
	}
}

func (p *operatorProcessor) stopAllOperators() {
	p.mu.Lock()
	keys := make([]types.NamespacedName, 0, len(p.operators))
	for key := range p.operators {
		keys = append(keys, key)
	}
	p.mu.Unlock()

	for _, key := range keys {
		p.deleteOperator(key)
	}
}

func (p *operatorProcessor) publishStatus(spec *opv1a1.Operator, status opv1a1.OperatorStatus) error {
	obj := spec.DeepCopy()
	obj.Status = status

	unstructuredObj, err := apiruntime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return fmt.Errorf("convert operator status to unstructured: %w", err)
	}

	zs := zset.New()
	zs.Insert(dbunstructured.New(unstructuredObj, nil), 1)

	return p.Publish(dbspruntime.Event{Name: p.outputTopic, Data: zs})
}

func decodeOperator(doc any) (*opv1a1.Operator, error) {
	udoc, ok := doc.(*dbunstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unsupported document type %T", doc)
	}

	obj := &opv1a1.Operator{}
	if err := apiruntime.DefaultUnstructuredConverter.FromUnstructured(udoc.Fields(), obj); err != nil {
		return nil, fmt.Errorf("convert unstructured document: %w", err)
	}

	if obj.GetName() == "" {
		return nil, fmt.Errorf("operator object is missing metadata.name")
	}

	return obj, nil
}

func failedOperatorStatus(gen int64, err error) opv1a1.OperatorStatus {
	status := opv1a1.OperatorStatus{}
	status.LastErrors = []string{err.Error()}

	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               string(opv1a1.OperatorConditionReady),
		Status:             metav1.ConditionFalse,
		Reason:             string(opv1a1.OperatorReasonNotReady),
		ObservedGeneration: gen,
		LastTransitionTime: metav1.Now(),
		Message:            "failed to initialize operator",
	})

	return status
}

func errorsJoin(a, b error) error {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return fmt.Errorf("%v; %w", a, b)
}
