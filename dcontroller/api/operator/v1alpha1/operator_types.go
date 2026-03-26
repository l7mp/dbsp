package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	SchemeBuilder.Register(&Operator{}, &OperatorList{})
}

// Operator is an abstraction of a basic unit of automation, a set of related controllers working
// on a single shared view of resources.
//
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:categories=dcontroller,scope=Cluster,shortName=operators
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// // +kubebuilder:printcolumn:name="ControllerNum",type=integer,JSONPath=`length(.spec.controllers)`
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

// OperatorSpec defines the desired state of an operator.
type OperatorSpec struct {
	// Controllers is a list of controllers that collectively implement the operator.
	//
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=255
	Controllers []Controller `json:"controllers"`
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
