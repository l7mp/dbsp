package engine

import (
	"github.com/l7mp/dbsp/compiler"
	"github.com/l7mp/dbsp/dbsp/zset"
)

// Engine manages the lifecycle of a DBSP incremental computation.
type Engine interface {
	// Compile compiles a query source. If the engine is configured for
	// incremental mode, the circuit is transformed using Algorithm 6.4.
	Compile(source string) error

	// Start prepares the engine for execution. Must be called after Compile.
	Start() error

	// Step executes one timestep. Input keys are logical names as defined by the
	// Compiler. Returns output Z-sets keyed by logical output names.
	Step(inputs map[string]zset.ZSet) (map[string]zset.ZSet, error)

	// Reset clears all internal state (integrators, differentiators, delays).
	Reset() error

	// Sync resynchronizes an incremental engine after Reset using SotW
	// reconciliation. For non-incremental engines, Sync is a no-op.
	Sync() error

	// Stop shuts down the engine and releases resources.
	Stop() error

	// IsIncremental returns true if the engine operates in incremental mode.
	IsIncremental() bool

	// CompiledQuery returns the compiled query, or nil if not yet compiled.
	CompiledQuery() *compiler.Query
}
