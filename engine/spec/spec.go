// Package spec defines the serialized operator format: the single shape a
// pipeline program freezes into and loads from. The dcontroller Operator
// CRD types alias the types here, so a spec saved by one frontend (a
// JavaScript freezer, a hand-written YAML) loads unchanged through the
// other (kubectl apply + dcontroller).
//
// The package is connector-agnostic: sources and targets name resources by
// group/version/kind, and the resource GROUP routes the binding to a
// connector (absent or Kubernetes API groups bind through the Kubernetes
// connector, the operator's own view group binds internal retained topics,
// a connector-owned group such as "xds.dcontroller.io" binds through that
// connector). Connector-specific residue rides in the schemaless
// labelSelector/predicate/parameters fields and is interpreted by the
// connector that binds it.
package spec

import (
	"encoding/json"
	"fmt"

	"github.com/l7mp/dbsp/engine/transform"
)

// OperatorSpec is the top-level serialized operator: a named set of
// controllers sharing one view space.
type OperatorSpec struct {
	// Controllers are the controllers of the operator.
	Controllers []Controller `json:"controllers"`
}

// Controller is a translator that processes a set of source resources via
// a declarative pipeline into deltas on target resources. A controller is
// defined by a name, its sources, a processing program (exactly one of
// pipeline, sql, or circuit), its targets, and an optional transform list.
type Controller struct {
	// Name is the unique name of the controller.
	Name string `json:"name"`

	// Sources are the resources the controller watches.
	Sources []Source `json:"sources"`

	// Pipeline is an aggregation pipeline applied to the source deltas.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Pipeline *json.RawMessage `json:"pipeline,omitempty"`

	// SQL is an SQL query applied to the source deltas.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	SQL *json.RawMessage `json:"sql,omitempty"`

	// Circuit is a hand-built dbsp circuit.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Circuit *json.RawMessage `json:"circuit,omitempty"`

	// Targets are the resource endpoints the results are written to.
	Targets []Target `json:"targets"`

	// Transforms is the list of circuit transforms applied to the compiled
	// program, in canonical order (the engine orders the set; listing
	// states what the controller is, not the order). An absent list means
	// the default chain: Reconciler, Distincter, Incrementalizer.
	//
	// +optional
	Transforms []transform.TransformSpec `json:"transforms,omitempty"`
}

// Resource names a resource by group, version and kind. The group routes
// the binding to a connector; an absent group with an absent version
// defaults to the operator's own view group.
type Resource struct {
	// Group is the API group. Default is "<operator>.view.dcontroller.io".
	//
	// +optional
	Group *string `json:"apiGroup,omitempty"`
	// Version is the version of the resource.
	//
	// +optional
	Version *string `json:"version,omitempty"`
	// Kind is the type of the resource. Mandatory.
	Kind string `json:"kind"`
}

// Source is a source that feeds deltas into the controller.
type Source struct {
	Resource `json:",inline"`
	// As is the pipeline-facing stream name (the @inputs name the
	// pipeline references); it defaults to Kind and matters when several
	// sources share a kind or the stream name and the resource kind
	// differ.
	//
	// +optional
	As string `json:"as,omitempty"`
	// Type names the binding connector's producer verb; the default is
	// the connector's canonical one (Kubernetes and xds: Watcher, misc:
	// Tick).
	//
	// +optional
	Type SourceType `json:"type,omitempty"`
	// Level switches a Watcher source from delta ingest to level ingest:
	// every event carries the full snapshot instead of the increment.
	// Ignored on sources with no level mode.
	//
	// +optional
	Level bool `json:"level,omitempty"`
	// Namespace, if given, restricts the source to events from that
	// namespace.
	//
	// +optional
	Namespace *string `json:"namespace,omitempty"`
	// LabelSelector is an optional label selector filtering events on
	// this source, interpreted by the binding connector (Kubernetes
	// LabelSelector semantics: matchLabels plus matchExpressions).
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	LabelSelector *json.RawMessage `json:"labelSelector,omitempty"`
	// Predicate is an event-filtering predicate on this source,
	// interpreted by the binding connector.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Predicate *json.RawMessage `json:"predicate,omitempty"`
	// Parameters carries source-specific parameters, interpreted by the
	// binding connector: Periodic sources take {"period": "5m"}, xds
	// sources the upstream server address.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Parameters *json.RawMessage `json:"parameters,omitempty"`
}

// SourceType represents the type of a source.
type SourceType string

const (
	// Watcher watches a resource and feeds deltas (or full snapshots
	// with level: true).
	Watcher SourceType = "Watcher"
	// Tick emits a trigger document every parameters.period (the misc
	// connector's Timer kind); each emission retracts the previous
	// trigger, and parameters.name distinguishes the timers sharing the
	// one Timer stream.
	Tick SourceType = "Tick"
	// Init emits a single trigger document at startup (the misc
	// connector's Timer kind).
	Init SourceType = "Init"
)

// Target is the resource endpoint a controller writes.
type Target struct {
	Resource `json:",inline"`
	// As is the pipeline-facing stream name (the @output name the
	// pipeline emits); it defaults to Kind and matters when the stream
	// name and the resource kind differ (a status fragment stream
	// "GatewayStatus" written to kind Gateway, say).
	//
	// +optional
	As string `json:"as,omitempty"`
	// Type is the type of the target. Default is Updater.
	//
	// +optional
	Type TargetType `json:"type,omitempty"`
	// Level switches an Updater target from delta writes to
	// state-of-the-world ownership: the controller writes the full
	// desired set of the kind and owns every object of it. Ignored on
	// targets with no level mode.
	//
	// +optional
	Level bool `json:"level,omitempty"`
	// Parameters carries target-specific parameters, interpreted by the
	// binding connector: xds targets take the egress server address.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Parameters *json.RawMessage `json:"parameters,omitempty"`
}

// TargetType represents the type of a target.
type TargetType string

const (
	// Updater overwrites the target resource with the result (or owns
	// the whole kind with level: true).
	Updater TargetType = "Updater"
	// Patcher applies the result as a patch to the target resource.
	Patcher TargetType = "Patcher"
)

// Validate checks the structural invariants a serialized controller must
// satisfy; connector-specific fields are validated by the binding
// connector at load time.
func (c *Controller) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("controller: name is required")
	}
	programs := 0
	for _, p := range []*json.RawMessage{c.Pipeline, c.SQL, c.Circuit} {
		if p != nil {
			programs++
		}
	}
	if programs != 1 {
		return fmt.Errorf("controller %q: exactly one of pipeline, sql, or circuit must be set", c.Name)
	}
	for i, s := range c.Sources {
		if s.Kind == "" {
			return fmt.Errorf("controller %q: source %d: kind is required", c.Name, i)
		}
	}
	for i, t := range c.Targets {
		if t.Kind == "" {
			return fmt.Errorf("controller %q: target %d: kind is required", c.Name, i)
		}
	}
	if _, err := transform.NewChainFromSpecs(c.Transforms); err != nil && len(c.Transforms) > 0 {
		return fmt.Errorf("controller %q: %w", c.Name, err)
	}
	return nil
}

// Validate checks every controller of the operator.
func (s *OperatorSpec) Validate() error {
	if len(s.Controllers) == 0 {
		return fmt.Errorf("operator: at least one controller is required")
	}
	for i := range s.Controllers {
		if err := s.Controllers[i].Validate(); err != nil {
			return fmt.Errorf("controller %d: %w", i, err)
		}
	}
	return nil
}
