package js

import (
	"github.com/go-logr/logr"
)

// Options configures a VM. Zero values are valid: a VM constructed with
// Options{} is equivalent to one built with NewVM(logr.Discard()).
type Options struct {
	// Logger is the structured logger. If unset, logs are discarded.
	Logger logr.Logger
}
