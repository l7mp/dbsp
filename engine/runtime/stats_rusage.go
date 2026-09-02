//go:build unix

package runtime

import "syscall"

// readRusage fills the process CPU time and peak RSS from getrusage.
func readRusage(rs *RuntimeStats) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return
	}
	rs.CPUUserSeconds = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	rs.CPUSystemSeconds = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	// Linux reports ru_maxrss in kilobytes.
	rs.MaxRSSBytes = uint64(ru.Maxrss) * 1024
}
