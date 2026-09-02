//go:build !unix

package runtime

// readRusage is a no-op where the platform has no rusage.
func readRusage(_ *RuntimeStats) {}
