package js

import (
	"github.com/go-logr/logr"
)

// Options configures a VM. Zero values are valid: a VM constructed with
// Options{} is equivalent to one built with NewVM(logr.Discard()).
type Options struct {
	// Logger is the structured logger. If unset, logs are discarded.
	Logger logr.Logger
	// StdlibPaths overrides stdlib module discovery. When non-empty the VM
	// searches only these directories (in order) for named require() calls.
	// When empty, auto-discovery is used: DBSP_STDLIB env var → binary-relative
	// → cwd-relative fallbacks.
	StdlibPaths []string
}
