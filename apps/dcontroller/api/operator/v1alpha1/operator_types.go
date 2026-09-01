package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func init() {
	SchemeBuilder.Register(
		func(scheme *runtime.Scheme) error {
			scheme.AddKnownTypes(GroupVersion,
				&Operator{},
				&OperatorList{},
			)
			return nil
		},
	)
}

// Operator is an abstraction of a basic unit of automation: a frozen
// DBSP runtime, a set of sources feeding streams, circuits processing
// them, and targets consuming them, sharing a single view space.
//
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:categories=dcontroller,scope=Cluster,shortName=operators
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// // +kubebuilder:printcolumn:name="CircuitNum",type=integer,JSONPath=`length(.spec.circuits)`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Operator struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of an operator.
	Spec OperatorSpec `json:"spec"`

	// Status defines the current state of the operator.
	Status OperatorStatus `json:"status,omitempty"`
}

// OperatorSpec defines the desired state of an operator: the serialized
// runtime it loads as.
type OperatorSpec struct {
	// Sources are the bindings feeding the runtime's streams.
	//
	// +optional
	Sources []Source `json:"sources,omitempty"`

	// Circuits are the runtime's circuits.
	//
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=255
	Circuits []CircuitSpec `json:"circuits"`

	// Targets are the bindings consuming the runtime's streams.
	//
	// +optional
	Targets []Target `json:"targets,omitempty"`
}

// +kubebuilder:object:root=true

// OperatorList contains a list of operators.
type OperatorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Operator `json:"items"`
}

// OperatorStatus specifies the status of an operator.
type OperatorStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	LastErrors []string           `json:"lastErrors,omitempty"`
}

// OperatorConditionType is a type of condition associated with an Operator. This type should be
// used with the OperatorStatus.Conditions field.
type OperatorConditionType string

// OperatorConditionReason defines the set of reasons that explain why a particular Operator
// condition type has been raised.
type OperatorConditionReason string

const (
	// The Ready condition is set if the Operator is running and each controller actively
	// reconciles resources.
	//
	// Possible reasons for this condition to be true are:
	//
	// * "Ready"
	//
	// Possible reasons for this condition to be False are:
	//
	// * "ReconcileError"
	//
	// Operators may raise this condition with other reasons, but should prefer to use the
	// reasons listed above to improve interoperability.

	// OperatorConditionReady represents the Ready condition.
	OperatorConditionReady OperatorConditionType = "Ready"

	// OperatorReasonReady is used with the "Ready" condition when the condition is true.
	OperatorReasonReady OperatorConditionReason = "Ready"

	// OperatorReasonReconciliationFailed is used with the "Ready" condition when
	// reconciliation has failed for at least one controller.
	OperatorReasonReconciliationFailed OperatorConditionReason = "ReconciliationFailed"

	// OperatorReasonNotReady is used with the "Ready" condition when the operator is not ready
	// for processing events.
	OperatorReasonNotReady OperatorConditionReason = "NotReady"
)
