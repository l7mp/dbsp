package xds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/go-logr/logr"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// consumerConfig is the shared configuration of the egress consumers (Updater,
// Setter).
type consumerConfig struct {
	// Name is the unique runtime component name. Required.
	Name string
	// OutputName is the runtime topic carrying the resource documents. Required.
	OutputName string
	// Type is the xDS type id (lds, cds, rds, eds, sds, ...), validated against
	// the registry at construction.
	Type   string
	Logger logr.Logger
}

// UpdaterConfig configures an Updater.
type UpdaterConfig = consumerConfig

// SetterConfig configures a Setter.
type SetterConfig = consumerConfig

// consumerBase holds the state shared by the egress consumers: the subscriber
// event loop, the target cache, and the resolved xDS type.
type consumerBase struct {
	*dbspruntime.BaseConsumer

	entry typeEntry
	cache *cachev3.LinearCache
	topic string
	typ   string
}

// newConsumerBase resolves the xDS type against the registry, binds the consumer
// to that type's cache in the Server, and wires the subscriber event loop.
func (s *Server) newConsumerBase(cfg consumerConfig, componentType string) (*consumerBase, error) {
	entry, ok := lookupType(cfg.Type)
	if !ok {
		return nil, fmt.Errorf("xds: unknown type %q (known: %v)", cfg.Type, KnownTypes())
	}
	lc, ok := s.caches[entry.url]
	if !ok {
		return nil, fmt.Errorf("xds: server has no cache for type %q", cfg.Type)
	}
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName(componentType).WithValues("type", cfg.Type, "topic", cfg.OutputName)

	base, err := dbspruntime.NewBaseConsumer(dbspruntime.BaseConsumerConfig{
		Name:          cfg.Name,
		Subscriber:    s.rt.NewSubscriber(),
		ErrorReporter: s.rt,
		Logger:        log,
		Topics:        []string{cfg.OutputName},
	})
	if err != nil {
		return nil, err
	}

	return &consumerBase{
		BaseConsumer: base,
		entry:        entry,
		cache:        lc,
		topic:        cfg.OutputName,
		typ:          cfg.Type,
	}, nil
}

// Updater subscribes to one output topic and applies its documents to the
// Server's shared cache as an incremental delta: each event is netted per name
// and written with a single LinearCache.UpdateResources (positive residual ->
// upsert, negative-only -> delete). This is the DBSP-native path and it is
// stateless: the cache holds the accumulated state. It is a runtime Runnable.
type Updater struct {
	*consumerBase
}

// Updater builds an Updater that writes the named output topic into this
// Server's cache as the configured xDS type. Pass the result to Runtime.Add.
func (s *Server) Updater(cfg UpdaterConfig) (*Updater, error) {
	base, err := s.newConsumerBase(cfg, "xds-updater")
	if err != nil {
		return nil, err
	}
	return &Updater{consumerBase: base}, nil
}

// Start runs the consumer event loop until ctx is cancelled.
func (u *Updater) Start(ctx context.Context) error {
	return u.Run(ctx, u)
}

// Consume nets one delta into resource operations and applies them to the
// cache in a single batched update. Keys the fold rejects are reported and
// dropped; the rest of the delta still applies.
func (u *Updater) Consume(_ context.Context, ev dbspruntime.Event) error {
	ops, kerrs := classify(u.entry, ev.Data)
	errs := make([]error, 0, len(kerrs)+1)
	for _, ke := range kerrs {
		errs = append(errs, ke)
	}
	if len(ops.upserts) > 0 || len(ops.deletes) > 0 {
		if err := u.cache.UpdateResources(ops.upserts, ops.deletes); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// MarshalJSON provides a stable machine-readable representation.
func (u *Updater) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"component": "consumer",
		"type":      "xds",
		"mode":      "updater",
		"xdsType":   u.typ,
		"name":      u.Name(),
		"topic":     u.topic,
	})
}
