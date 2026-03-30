package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
)

type ControllerType string

const (
	ControllerTypeIncremental = "Incremental"
	ControllerTypeSOTW        = "StateOfTheWorld"
)

// Controller is a translator that processes a set of base resources via a declarative pipeline
// into deltas on target resources. A controller is defined by a name, a set of sources, a
// processing pipeline, and one or more targets.
type Controller struct {
	// Name is the unique name of the controller.
	Name string `json:"name"`

	// Type is the type of the controller. Default is Incremental.
	//
	// +kubebuilder:validation:Enum=Incremental;StateOfTheWorld
	// +kubebuilder:default=Incremental
	Type ControllerType `json:"type,omitempty"`

	// The base resource(s) the controller watches.
	Sources []Source `json:"sources"`

	// Pipeline is an processing pipeline applied to base objects. At least one of a Pipeline,
	// a Circuit or an SQL query must be specified.
	//
	// +optional
	Pipeline *apiextensionsv1.JSON `json:"pipeline,omitempty"`

	// SQL is an SQL query applied to base objects. At least one of a Pipeline, a Circuit or an
	// SQL query must be specified.
	//
	// +optional
	SQL *apiextensionsv1.JSON `json:"sql,omitempty"`

	// Circuit is a compiler dbsp Circuit applied to base objects. At least one of a Pipeline,
	// a Circuit or an SQL query must be specified.
	//
	// +optional
	Circuit *apiextensionsv1.JSON `json:"circuit,omitempty"`

	// Targets are the resource endpoints where results are written.
	Targets []Target `json:"targets"`

	// Options control optional controller compilation/runtime transforms.
	//
	// +optional
	Options *ControllerOptions `json:"options,omitempty"`
}

// ControllerOptions controls optional transform passes.
type ControllerOptions struct {
	// DisableIncrementalizer disables incrementalization and keeps snapshot
	// (non-delta) circuit execution.
	//
	// +optional
	DisableIncrementalizer bool `json:"disableIncrementalizer,omitempty"`

	// DisableReconciler disables the reconciler transform pass.
	//
	// +optional
	DisableReconciler bool `json:"disableReconciler,omitempty"`

	// DisableRegularizer disables the regularizer transform pass.
	//
	// +optional
	DisableRegularizer bool `json:"disableRegularizer,omitempty"`
}

// Resource specifies a resource by the GVK.
type Resource struct {
	// Group is the API group. Default is "<operator-name>.view.dcontroller.io", where
	// <operator-name> is the name of the operator that manages the object.
	Group *string `json:"apiGroup,omitempty"`
	// Version is the version of the resource. Optional.
	Version *string `json:"version,omitempty"`
	// Kind is the type of the resource. Mandatory.
	Kind string `json:"kind"`
}

// Source is a watch source that feeds deltas into the controller.
type Source struct {
	Resource `json:",inline"`
	// Type specifies the behavior of the source. Default is Watcher.
	Type SourceType `json:"type,omitempty"`
	// Namespace, if given, restricts the source to generate events only from the given namespace.
	Namespace *string `json:"namespace,omitempty"`
	// LabelSelector is an optional label selector to filter events on this source.
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`
	// Predicate is a controller runtime predicate for filtering events on this source.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	Predicate *predicate.Predicate `json:"predicate,omitempty"`
	// Parameters contains arbitrary source-specific parameters for virtual sources.
	// For example, Periodic sources use {"period": "5m"}.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Parameters *apiextensionsv1.JSON `json:"parameters,omitempty"`
}

// SourceType represents the type of a source.
type SourceType string

const (
	// Watcher is a source that watches Kubernetes resources and performs incremental reconciliation.
	Watcher SourceType = "Watcher"
	// Lister is a source that watches Kubernetes resources and emits full snapshots on each watch event.
	Lister SourceType = "Lister"
	// Periodic is a source that triggers state-of-the-world reconciliation for all other sources.
	Periodic SourceType = "Periodic"
	// OneShot is a source that emits a single empty object for initialization.
	OneShot SourceType = "OneShot"
)

// Target is the target reource type in which the controller writes.
type Target struct {
	Resource `json:",inline"`
	// Type is the type of the target.
	Type TargetType `json:"type,omitempty"`
}

// TargetType represents the type of a target.
type TargetType string

const (
	// Updater is a target that will fully overwrite the target resource with the update.
	Updater TargetType = "Updater"
	// Patcher is a target that applies the update as a patch to the target resource.
	Patcher TargetType = "Patcher"
)
