package store

import toolscache "k8s.io/client-go/tools/cache"

// closedChan is a pre-closed channel shared by every syncedChecker.
var closedChan = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// syncedChecker is a cache.DoneChecker that is already done. The store's
// informers serve a local, in-memory view and never perform an asynchronous
// LIST/WATCH against an API server, so they are synced from birth: a caller
// waiting on HasSyncedChecker never blocks.
type syncedChecker struct{ name string }

func (s syncedChecker) Name() string          { return s.name }
func (s syncedChecker) Done() <-chan struct{} { return closedChan }

var _ toolscache.DoneChecker = syncedChecker{}
