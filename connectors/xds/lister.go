package xds

import (
	"context"
	"encoding/json"
	"fmt"

	sotw "github.com/envoyproxy/go-control-plane/pkg/client/sotw/v3"
	"google.golang.org/protobuf/types/known/anypb"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// Lister ingests an upstream xDS type over State-of-the-World ADS and republishes
// it as DBSP inputs. Each SotW response is a full resource set, so the Lister
// emits a snapshot Z-set (every resource weight +1): pair it with a Setter (or a
// snapshot-consuming pipeline). SotW is the universal xDS protocol, so a Lister
// works against any upstream. It is a runtime Runnable.
type Lister struct {
	*producerBase
}

var _ dbspruntime.Producer = (*Lister)(nil)

// NewLister builds a Lister ingesting cfg.Type from cfg.Address. Name uniqueness
// is enforced when the Lister is passed to Runtime.Add.
func NewLister(cfg ListerConfig) (*Lister, error) {
	base, err := newProducerBase(cfg, "xds-lister")
	if err != nil {
		return nil, err
	}
	return &Lister{producerBase: base}, nil
}

// Start dials the upstream, opens a SotW ADS stream for the configured type, and
// republishes each snapshot until ctx is cancelled.
func (l *Lister) Start(ctx context.Context) error {
	conn, err := l.dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	client := sotw.NewADSClient(ctx, l.node, l.entry.url)
	if err := client.InitConnect(conn); err != nil {
		return fmt.Errorf("xds: lister connect: %w", err)
	}
	l.log.V(2).Info("lister connected")

	for {
		resp, err := client.Fetch()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("xds: lister fetch: %w", err)
		}

		zs, err := l.snapshotZSet(resp.Resources)
		if err != nil {
			// A malformed upstream resource is non-critical: report and keep the
			// stream alive by ACKing so the connection is not torn down.
			l.HandleError(err)
		} else if !zs.IsZero() {
			if err := l.emit(zs); err != nil {
				l.HandleError(err)
			}
		}

		if err := client.Ack(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("xds: lister ack: %w", err)
		}
	}
}

// snapshotZSet turns a SotW response's resources into a snapshot Z-set: every
// resource at weight +1.
func (l *Lister) snapshotZSet(resources []*anypb.Any) (zset.ZSet, error) {
	zs := zset.New()
	for _, any := range resources {
		doc, _, err := anyToDoc(any)
		if err != nil {
			return zset.New(), err
		}
		zs.Insert(doc, 1)
	}
	return zs, nil
}

// MarshalJSON provides a stable machine-readable representation.
func (l *Lister) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"component": "producer",
		"type":      "xds",
		"mode":      "lister",
		"xdsType":   l.typ,
		"name":      l.Name(),
		"topic":     l.topic,
		"upstream":  l.address,
	})
}
