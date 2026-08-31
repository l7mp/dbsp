package v1alpha1

import (
	"github.com/l7mp/dbsp/engine/spec"
)

// The controller-level types are the engine's serialized operator format
// (engine/spec) verbatim: a pipeline frozen by any frontend loads through
// this CRD unchanged, and a spec written for the CRD compiles anywhere the
// engine runs. See the spec package for the field documentation.

// Controller is a translator that processes a set of source resources via a
// declarative pipeline into deltas on target resources.
type Controller = spec.Controller

// Source is a source that feeds deltas into a controller.
type Source = spec.Source

// SourceType represents the type of a source.
type SourceType = spec.SourceType

// Target is the resource endpoint a controller writes.
type Target = spec.Target

// TargetType represents the type of a target.
type TargetType = spec.TargetType

// Resource names a resource by group, version and kind.
type Resource = spec.Resource

const (
	// Watcher watches a resource and feeds deltas (or full snapshots
	// with level: true).
	Watcher = spec.Watcher
	// Tick emits a trigger document every parameters.period.
	Tick = spec.Tick
	// Init emits a single trigger document at startup.
	Init = spec.Init

	// Updater overwrites the target resource with the result (or owns
	// the whole kind with level: true).
	Updater = spec.Updater
	// Patcher applies the result as a patch to the target resource.
	Patcher = spec.Patcher
)
