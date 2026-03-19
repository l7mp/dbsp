// Package runtime defines connector and lifecycle contracts for DBSP execution wiring.
//
// Input and output envelopes intentionally stay minimal. Runtime implementations
// may run either incremental (delta) or non-incremental (snapshot) engines, and
// callers are responsible for feeding a stream that matches the configured
// engine mode.
package runtime
