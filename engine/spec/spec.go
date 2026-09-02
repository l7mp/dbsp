// Package spec defines the serialized runtime format: the single shape a
// DBSP runtime freezes into and loads from. An operator is a frozen
// runtime, so the dcontroller Operator CRD types alias the types here and
// a spec saved by one frontend (a JavaScript freezer, a hand-written
// YAML) loads unchanged through the other (kubectl apply + dcontroller).
//
// The package is connector-agnostic: sources and targets name resources by
// group/version/kind, and the resource GROUP routes the binding to a
// connector (absent or Kubernetes API groups bind through the Kubernetes
// connector, the operator's own view group binds internal retained topics,
// a connector-owned group such as "xds.dcontroller.io" binds through that
// connector). Connector-specific residue rides in the schemaless
// labelSelector/predicate/parameters fields and is interpreted by the
// connector that binds it.
//
// Every stream carries deltas: connectors translate state-speaking
// remotes at their border, and a circuit that wants full states
// integrates them itself (a chain without the Incrementalizer runs as
// the snapshot program ∫ -> Q -> D). The runtime treats every topic as
// a delta stream; ship integrals across a topic only knowing that.
package spec

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/engine/transform"
)

// RuntimeSpec is the top-level serialized runtime, mirroring what a DBSP
// runtime is: a set of sources feeding streams, a set of circuits
// processing them, and a set of targets consuming them. Streams couple
// the sets by name: a source feeds the stream its `as` names (defaulting
// to the resource kind), a circuit names its input and output streams,
// and a target consumes likewise. A stream produced and consumed only by
// circuits is an internal wire with no binding at all.
type RuntimeSpec struct {
	// Sources are the bindings feeding the runtime's streams.
	//
	// +optional
	Sources []Source `json:"sources,omitempty"`

	// Circuits are the runtime's circuits.
	Circuits []CircuitSpec `json:"circuits"`

	// Targets are the bindings consuming the runtime's streams.
	//
	// +optional
	Targets []Target `json:"targets,omitempty"`
}

// CircuitSpec is one serialized circuit: a program over the runtime's
// streams, with the transform chain applied to it. The program is given
// in exactly one of the three languages: an aggregation pipeline, an SQL
// query, or a hand-built graph.
type CircuitSpec struct {
	// Name is the unique name of the circuit.
	Name string `json:"name"`

	// Inputs are the streams the circuit consumes. Omitted, it defaults
	// to the runtime's single source stream; with several sources the
	// list is required.
	//
	// +optional
	Inputs []string `json:"inputs,omitempty"`

	// Outputs are the streams the circuit produces. Omitted, it defaults
	// to the runtime's single target stream; with several targets the
	// list is required.
	//
	// +optional
	Outputs []string `json:"outputs,omitempty"`

	// Pipeline is an aggregation pipeline over the input streams.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Pipeline *json.RawMessage `json:"pipeline,omitempty"`

	// SQL is an SQL query over the input streams.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	SQL *json.RawMessage `json:"sql,omitempty"`

	// Graph is a hand-built dbsp circuit graph.
	//
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Graph *json.RawMessage `json:"graph,omitempty"`

	// Transforms is the list of circuit transforms applied to the
	// compiled program, in canonical order (the engine orders the set;
	// listing states what the circuit is, not the order). There are no
	// default transforms: an absent or empty list means none, and the
	// circuit runs as the snapshot program it compiles to.
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

// ParseResource parses the compact resource reference: "Kind" (the
// runtime's own view group), "apiVersion/Kind" (a core "v1/Pod" or a
// view-group apiVersion), or "group/version/Kind".
func ParseResource(ref string) (Resource, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return Resource{}, fmt.Errorf("missing resource reference")
	}
	parts := strings.Split(ref, "/")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	switch len(parts) {
	case 1:
		if parts[0] == "" {
			return Resource{}, fmt.Errorf("missing kind")
		}
		return Resource{Kind: parts[0]}, nil
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return Resource{}, fmt.Errorf("resource reference: expected apiVersion/Kind")
		}
		group, version := "", parts[0]
		if i := strings.LastIndex(parts[0], "."); i >= 0 {
			// An apiVersion with a dotted group, e.g.
			// "discovery.k8s.io/v1" cannot appear in the two-segment
			// form (the slash splits it); a bare version has no dot.
			return Resource{}, fmt.Errorf("resource reference %q: expected apiVersion/Kind or group/version/Kind", ref)
		}
		return Resource{Group: &group, Version: &version, Kind: parts[1]}, nil
	case 3:
		if parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return Resource{}, fmt.Errorf("resource reference: expected group/version/Kind")
		}
		return Resource{Group: &parts[0], Version: &parts[1], Kind: parts[2]}, nil
	default:
		return Resource{}, fmt.Errorf("resource reference %q: expected Kind, apiVersion/Kind or group/version/Kind", ref)
	}
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

// Validate checks the structural invariants a serialized circuit must
// satisfy; connector-specific fields are validated by the binding
// connector at assembly.
func (c *CircuitSpec) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("circuit: name is required")
	}
	programs := 0
	for _, p := range []*json.RawMessage{c.Pipeline, c.SQL, c.Graph} {
		if p != nil {
			programs++
		}
	}
	if programs != 1 {
		return fmt.Errorf("circuit %q: exactly one of pipeline, sql, or graph must be set", c.Name)
	}
	return nil
}

// Validate checks the structural invariants of the serialized runtime.
func (s *RuntimeSpec) Validate() error {
	if len(s.Circuits) == 0 {
		return fmt.Errorf("runtime: at least one circuit is required")
	}
	names := map[string]bool{}
	for i := range s.Circuits {
		if err := s.Circuits[i].Validate(); err != nil {
			return fmt.Errorf("circuit %d: %w", i, err)
		}
		if names[s.Circuits[i].Name] {
			return fmt.Errorf("circuit %q: duplicate name", s.Circuits[i].Name)
		}
		names[s.Circuits[i].Name] = true
	}
	for i, src := range s.Sources {
		if src.Kind == "" {
			return fmt.Errorf("source %d: kind is required", i)
		}
	}
	for i, t := range s.Targets {
		if t.Kind == "" {
			return fmt.Errorf("target %d: kind is required", i)
		}
	}
	return nil
}
