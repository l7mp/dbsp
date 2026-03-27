package operator

import (
	"context"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/apiserver"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	"github.com/l7mp/dbsp/dcontroller/controller"
	"github.com/l7mp/dbsp/engine/executor"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Config can be used to customize the Operator's behavior.
type Config struct {
	// Spec is the declarative operator specification.
	Spec opv1a1.OperatorSpec

	// K8sRuntime is the Kubernetes runtime providing the composite client and REST mapper.
	// Required for Watcher sources and Kubernetes consumers; may be nil if only virtual
	// sources (Periodic, OneShot) are used without Kubernetes consumers.
	K8sRuntime *k8sruntime.Runtime

	// Logger is used for structured logging.
	Logger logr.Logger
}

// Operator definition.
type Operator struct {
	name        string
	spec        opv1a1.OperatorSpec
	runtime     *dbspruntime.Runtime
	k8sruntime  *k8sruntime.Runtime
	controllers []*controller.Controller
	apiServer   *apiserver.APIServer
	errorChan   chan dbspruntime.Error
	errorStack  *ErrorStack
	logger, log logr.Logger
}

const defaultErrorStackCapacity = 10

// New creates a new operator with its own dedicated manager.
func New(name string, cfg Config) (*Operator, error) {
	logger := cfg.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	op := &Operator{
		name:        name,
		k8sruntime:  cfg.K8sRuntime,
		runtime:     dbspruntime.NewRuntime(logger.WithName("operator-runtime").WithValues("name", name)),
		spec:        cfg.Spec,
		apiServer:   cfg.K8sRuntime.GetAPIServer(),
		errorChan:   make(chan dbspruntime.Error, 64),
		errorStack:  NewErrorStack(defaultErrorStackCapacity),
		controllers: []*controller.Controller{},
		logger:      logger,
		log:         logger.WithName("operator").WithValues("name", name),
	}

	// Set the error channel of the dbsp runtime
	op.runtime.SetErrorChannel(op.errorChan)

	// Create the controllers for the operator
	for _, spec := range cfg.Spec.Controllers {
		c, err := controller.New(controller.Config{
			OperatorName: op.name,
			Spec:         spec,
			Runtime:      op.runtime,
			K8sRuntime:   op.k8sruntime,
			Logger:       op.logger,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create controller %s for operator %s: %w",
				spec.Name, op.name, err)
		}
		op.controllers = append(op.controllers, c)
	}

	if err := op.RegisterGVKs(); err != nil {
		return nil, fmt.Errorf("failed to register GVKs for operator %s: %w",
			op.name, err)
	}

	return op, nil
}

// NewFromFile creates a new operator from a serialized operator spec. Note that once this call
// finishes there is no way to add new controllers to the operator.
func NewFromFile(name string, file string, k8sRuntime *k8sruntime.Runtime, logger logr.Logger) (*Operator, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	var spec opv1a1.OperatorSpec
	if err := yaml.Unmarshal(b, &spec); err != nil {
		return nil, fmt.Errorf("failed to parse operator spec: %w", err)
	}

	return New(name, Config{Spec: spec, K8sRuntime: k8sRuntime, Logger: logger})
}

// GetName returns the name of the operator.
func (op *Operator) GetName() string {
	return op.name
}

// GetRuntime returns the runtime used by the operator.
func (op *Operator) GetRuntime() *dbspruntime.Runtime {
	return op.runtime
}

// SetControllerObserver installs an optional executor observer for one controller circuit.
// Returns true when a controller with the given name exists.
func (op *Operator) SetControllerObserver(controllerName string, observer executor.ObserverFunc) bool {
	return op.runtime.SetCircuitObserver(controllerName, observer)
}

// Start starts the operator's manager. This blocks until the context is cancelled.
// Controllers registered with the operator will be started automatically by the manager.
func (op *Operator) Start(ctx context.Context) error {
	op.log.V(2).Info("starting operator")

	go func() {
		for {
			select {
			case err, ok := <-op.errorChan:
				if !ok {
					return
				}
				if err.Err != nil {
					op.log.Error(err.Err, "runtime component error", "origin", err.Origin)
					op.errorStack.Push(err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	err := op.runtime.Start(ctx)
	op.log.V(2).Info("stopping operator")
	return err
}

// GetStatus populates the operator status with the controller statuses.
func (op *Operator) GetStatus(gen int64) opv1a1.OperatorStatus {
	errs := op.errorStack.Errors()

	ret := opv1a1.OperatorStatus{}
	if len(errs) == 0 {
		meta.SetStatusCondition(&ret.Conditions, metav1.Condition{
			Type:               string(opv1a1.OperatorConditionReady),
			Status:             metav1.ConditionTrue,
			Reason:             string(opv1a1.OperatorReasonReady),
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Message:            "controllers report no reconciliation error",
		})
	} else {
		meta.SetStatusCondition(&ret.Conditions, metav1.Condition{
			Type:               string(opv1a1.OperatorConditionReady),
			Status:             metav1.ConditionFalse,
			Reason:             string(opv1a1.OperatorReasonReconciliationFailed),
			ObservedGeneration: gen,
			LastTransitionTime: metav1.Now(),
			Message:            "reconciliation failed for at least one controller",
		})
		ret.LastErrors = make([]string, len(errs))
		for i, err := range errs {
			ret.LastErrors[i] = err.Error()
		}
	}

	return ret
}

// RegisterGVKs registers the view resources associated with the operator' controllers in the
// extension API server.
func (op *Operator) RegisterGVKs() error {
	if op.apiServer == nil {
		return nil
	}

	gvks := op.GetGVKs()

	op.log.V(2).Info("registering GVKs", "API group", viewv1a1.Group(op.name),
		"GVKs", gvks)

	return op.apiServer.RegisterGVKs(gvks)
}

// // UnregisterGVKs unregisters the view resources associated with the controllers.
func (op *Operator) UnregisterGVKs() {
	if op.apiServer == nil {
		return
	}

	op.log.V(2).Info("unregistering GVKs", "API group", viewv1a1.Group(op.name))

	op.apiServer.UnregisterGVKs(op.GetGVKs())
}

// GetGVKs returns the GVKs of this operator group.
func (op *Operator) GetGVKs() []schema.GroupVersionKind {
	gvks := make([]schema.GroupVersionKind, 0, len(op.controllers))
	for _, c := range op.controllers {
		gvks = append(gvks, c.GetGVKs()...)
	}

	var ret []schema.GroupVersionKind
	set := make(map[schema.GroupVersionKind]bool)
	for _, item := range gvks {
		if item.Group != viewv1a1.Group(op.name) {
			continue
		}
		if !set[item] {
			set[item] = true
			ret = append(ret, item)
		}
	}

	return ret
}
