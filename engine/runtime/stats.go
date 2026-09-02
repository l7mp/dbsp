package runtime

import (
	"maps"
	goruntime "runtime"
	"sync"
)

// Stats is a point-in-time snapshot of a runtime's observability surface:
// the runtime's own named event counters, the process-wide counters
// (events raised by host-level machinery shared across runtimes, the
// Kubernetes connector runtime first among them), and the Go runtime's
// memory and CPU figures. The JS surface exposes it as handle.stats(),
// with runtime.stats() bound to the default runtime.
type Stats struct {
	// Counters holds this runtime's named event counters.
	Counters map[string]int64 `json:"counters"`
	// ProcessCounters holds the process-wide named event counters.
	ProcessCounters map[string]int64 `json:"processCounters"`
	// Runtime holds the Go runtime figures (process-wide).
	Runtime RuntimeStats `json:"runtime"`
}

// RuntimeStats carries the Go runtime memory and CPU figures.
type RuntimeStats struct {
	Goroutines      int     `json:"goroutines"`
	HeapAllocBytes  uint64  `json:"heapAllocBytes"`
	HeapSysBytes    uint64  `json:"heapSysBytes"`
	TotalAllocBytes uint64  `json:"totalAllocBytes"`
	NumGC           uint32  `json:"numGC"`
	GCPauseTotalMs  float64 `json:"gcPauseTotalMs"`
	// Process CPU time and peak RSS, from getrusage (zero where the
	// platform has no rusage).
	CPUUserSeconds   float64 `json:"cpuUserSeconds"`
	CPUSystemSeconds float64 `json:"cpuSystemSeconds"`
	MaxRSSBytes      uint64  `json:"maxRSSBytes"`
}

// Process-wide counters, for events raised outside any runtime.
var (
	statsMu       sync.Mutex
	statsCounters = map[string]int64{}
)

// CountStat adds one to the named process-wide counter.
func CountStat(name string) {
	AddStat(name, 1)
}

// AddStat adds delta to the named process-wide counter.
func AddStat(name string, delta int64) {
	statsMu.Lock()
	defer statsMu.Unlock()
	statsCounters[name] += delta
}

// StatsSnapshot returns a copy of the process-wide counters.
func StatsSnapshot() map[string]int64 {
	statsMu.Lock()
	defer statsMu.Unlock()
	return maps.Clone(statsCounters)
}

// CountStat adds one to the runtime's named counter.
func (rt *Runtime) CountStat(name string) {
	rt.AddStat(name, 1)
}

// AddStat adds delta to the runtime's named counter.
func (rt *Runtime) AddStat(name string, delta int64) {
	rt.statsMu.Lock()
	defer rt.statsMu.Unlock()
	rt.stats[name] += delta
}

// Stats returns the runtime's full stats snapshot.
func (rt *Runtime) Stats() Stats {
	rt.statsMu.Lock()
	counters := maps.Clone(rt.stats)
	rt.statsMu.Unlock()

	var m goruntime.MemStats
	goruntime.ReadMemStats(&m)
	s := Stats{
		Counters:        counters,
		ProcessCounters: StatsSnapshot(),
		Runtime: RuntimeStats{
			Goroutines:      goruntime.NumGoroutine(),
			HeapAllocBytes:  m.HeapAlloc,
			HeapSysBytes:    m.HeapSys,
			TotalAllocBytes: m.TotalAlloc,
			NumGC:           m.NumGC,
			GCPauseTotalMs:  float64(m.PauseTotalNs) / 1e6,
		},
	}
	readRusage(&s.Runtime)
	return s
}
