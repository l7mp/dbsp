package xds

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"

	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// Watcher ingests an upstream xDS type over incremental (Delta) ADS and
// republishes it as DBSP delta inputs. It is a minimal hand-rolled Delta client
// (go-control-plane ships none): it subscribes wildcard, and for each response
// emits a delta Z-set: added/changed resources as -old,+new and removals as
// -old. To synthesise the -old on an update (the Delta protocol re-sends a
// changed resource with no retraction) it keeps a last-seen document per name.
// Delta ADS is optional upstream; use a Lister for SotW-only control planes. It
// is a runtime Runnable.
type Watcher struct {
	*producerBase

	lastSeen map[string]*unstructured.Unstructured
}

var _ dbspruntime.Producer = (*Watcher)(nil)

// NewWatcher builds a Watcher ingesting cfg.Type from cfg.Address. Name
// uniqueness is enforced when the Watcher is passed to Runtime.Add.
func NewWatcher(cfg WatcherConfig) (*Watcher, error) {
	base, err := newProducerBase(cfg, "xds-watcher")
	if err != nil {
		return nil, err
	}
	return &Watcher{producerBase: base, lastSeen: map[string]*unstructured.Unstructured{}}, nil
}

// Start consumes Delta ADS responses until ctx is cancelled. The connection
// and the stream are re-established on failure: an upstream restart is
// routine, not the end of the subscription. Delta ADS has no cross-stream
// cursor (nonces are per-stream), so recovery is a state resync: the fresh
// stream's initial wildcard sync is diffed against the last-seen state,
// retracting resources that disappeared during the outage and re-emitting
// only what actually changed. A failure before any response ever arrived
// fails fast instead: that is a misconfiguration, not an outage.
func (w *Watcher) Start(ctx context.Context) error {
	everReceived := false
	// Manual doubling backoff: this module carries no Kubernetes
	// dependencies, so apimachinery's wait.Backoff stays out.
	backoff := time.Second

	for {
		received, err := w.stream(ctx)
		everReceived = everReceived || received
		if ctx.Err() != nil {
			return nil //nolint:nilerr // shutdown, not a stream failure
		}
		if !everReceived {
			return err
		}
		if received {
			backoff = time.Second
		}
		w.HandleError(fmt.Errorf("xds: watcher stream failed, reconnecting in %s: %w", backoff, err))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// stream runs one connection lifetime: dial, subscribe wildcard, consume
// until the stream breaks. Reports whether any response arrived.
func (w *Watcher) stream(ctx context.Context) (bool, error) {
	conn, err := w.dial()
	if err != nil {
		return false, err
	}
	defer conn.Close()

	stream, err := discoverygrpc.NewAggregatedDiscoveryServiceClient(conn).DeltaAggregatedResources(ctx)
	if err != nil {
		return false, fmt.Errorf("xds: watcher stream: %w", err)
	}

	if err := stream.Send(&discoverygrpc.DeltaDiscoveryRequest{
		Node:                   w.node,
		TypeUrl:                w.entry.url,
		ResourceNamesSubscribe: w.subscription(),
	}); err != nil {
		return false, fmt.Errorf("xds: watcher subscribe: %w", err)
	}
	w.log.V(2).Info("watcher subscribed")

	received := false
	// The first response of a (re)opened stream is the full wildcard sync:
	// a resource that vanished while the stream was down appears in neither
	// the resources nor the removals, so that response is diffed as a
	// snapshot against the last-seen state.
	resync := true
	for {
		resp, err := stream.Recv()
		if err != nil {
			return received, fmt.Errorf("xds: watcher recv: %w", err)
		}
		received = true

		zs, err := w.deltaZSet(resp, resync)
		resync = false
		if err != nil {
			w.HandleError(err)
		} else if !zs.IsZero() {
			if err := w.emit(zs); err != nil {
				w.HandleError(err)
			}
		}

		// ACK the response by echoing its nonce; a NACK path is not
		// implemented (a malformed upstream resource is reported and the
		// stream continues).
		if err := stream.Send(&discoverygrpc.DeltaDiscoveryRequest{
			TypeUrl:       w.entry.url,
			ResponseNonce: resp.GetNonce(),
		}); err != nil {
			return received, fmt.Errorf("xds: watcher ack: %w", err)
		}
	}
}

// deltaZSet turns one Delta response into a DBSP delta: each changed resource as
// -old (if last seen) then +new, and each removed name as -old. The last-seen
// map is updated in step. In resync mode (the initial sync of a reopened
// stream) the response is treated as a complete snapshot: last-seen
// resources it does not name are retracted.
func (w *Watcher) deltaZSet(resp *discoverygrpc.DeltaDiscoveryResponse, resync bool) (zset.ZSet, error) {
	zs := zset.New()
	seen := map[string]bool{}

	for _, r := range resp.GetResources() {
		doc, name, err := anyToDoc(r.GetResource())
		if err != nil {
			return zset.New(), err
		}
		if name == "" {
			name = r.GetName()
		}
		seen[name] = true
		if old, ok := w.lastSeen[name]; ok {
			zs.Insert(old, -1)
		}
		zs.Insert(doc, 1)
		w.lastSeen[name] = doc
	}

	for _, name := range resp.GetRemovedResources() {
		if old, ok := w.lastSeen[name]; ok {
			zs.Insert(old, -1)
			delete(w.lastSeen, name)
		}
	}

	if resync {
		for name, old := range w.lastSeen {
			if !seen[name] {
				zs.Insert(old, -1)
				delete(w.lastSeen, name)
			}
		}
	}

	return zs, nil
}

// MarshalJSON provides a stable machine-readable representation.
func (w *Watcher) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"component": "producer",
		"type":      "xds",
		"mode":      "watcher",
		"xdsType":   w.typ,
		"name":      w.Name(),
		"topic":     w.topic,
		"upstream":  w.address,
	})
}
