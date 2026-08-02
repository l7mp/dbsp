package xds

import (
	"context"
	"encoding/json"
	"errors"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Setter subscribes to one output topic and applies its documents to the
// Server's shared cache as a full snapshot: each event is treated as the
// complete desired set for its xDS type and written with a single
// LinearCache.SetResources. Deletion is by omission: a name absent from an
// event is removed. Use a Setter when its input topic carries snapshots (a
// snapshot-producing pipeline); use an Updater for the DBSP-native delta case.
// It is a runtime Runnable.
type Setter struct {
	*consumerBase
}

// Setter builds a Setter that writes the named output topic into this Server's
// cache as the configured xDS type. Pass the result to Runtime.Add.
func (s *Server) Setter(cfg SetterConfig) (*Setter, error) {
	base, err := s.newConsumerBase(cfg, "xds-setter")
	if err != nil {
		return nil, err
	}
	return &Setter{consumerBase: base}, nil
}

// Start runs the consumer event loop until ctx is cancelled.
func (s *Setter) Start(ctx context.Context) error {
	return s.Run(ctx, s)
}

// Consume treats the event as the full desired set and replaces the type's
// resources in one LinearCache.SetResources. An empty snapshot clears the
// type. A snapshot with fold or decode failures is not applied at all:
// deletion is by omission, so writing the surviving subset would silently
// delete the failed resources.
func (s *Setter) Consume(_ context.Context, ev dbspruntime.Event) error {
	set, kerrs := snapshot(s.entry, ev.Data)
	if len(kerrs) > 0 {
		errs := make([]error, 0, len(kerrs))
		for _, ke := range kerrs {
			errs = append(errs, ke)
		}
		return errors.Join(errs...)
	}
	s.cache.SetResources(set)
	return nil
}

// MarshalJSON provides a stable machine-readable representation.
func (s *Setter) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"component": "consumer",
		"type":      "xds",
		"mode":      "setter",
		"xdsType":   s.typ,
		"name":      s.Name(),
		"topic":     s.topic,
	})
}
