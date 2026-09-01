package v1alpha1

import (
	"github.com/l7mp/dbsp/engine/spec"
)

// The runtime-level types are the engine's serialized runtime format
// (engine/spec) verbatim: an operator is a frozen DBSP runtime, so a
// runtime frozen by any frontend loads through this CRD unchanged, and a
// spec written for the CRD assembles anywhere the engine runs. See the
// spec package for the field documentation.

// CircuitSpec is one serialized circuit: a program over the runtime's
// streams, with its transform chain.
type CircuitSpec = spec.CircuitSpec

// Source is a binding feeding one of the runtime's streams.
type Source = spec.Source

// SourceType represents the type of a source.
type SourceType = spec.SourceType

// Target is a binding consuming one of the runtime's streams.
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
