package xds

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// defaultNode is the xDS node id used when ProducerConfig.Node is empty.
const defaultNode = "dbsp"

// ProducerConfig is the shared configuration of the ingest producers (Lister,
// Watcher). Both dial an upstream xDS management server and republish its
// resources as DBSP inputs.
type ProducerConfig struct {
	// Name is the unique runtime component name. Required.
	Name string
	// InputName is the runtime topic the producer publishes to. Required.
	InputName string
	// Type is the xDS type id (lds, cds, rds, eds, sds, ...), validated against
	// the registry at construction. The upstream client subscribes to this one
	// type (the SotW client is per type URL), so one producer carries one type.
	Type string
	// Address is the upstream xDS server, e.g. "upstream:18000". Required.
	Address string
	// Node is the xDS node id presented to the upstream. Defaults to "dbsp".
	Node string
	// NodeCluster is the node's cluster field. Management servers use it for
	// snapshot scoping (envoy-gateway matches it against the IR key).
	NodeCluster string
	// NodeMetadata is the node's metadata struct. Management servers use it
	// for workload identity and config scoping (istiod reads NAMESPACE,
	// CLUSTER_ID and LABELS: a gateway proxy binds to its Gateway through
	// the gateway.networking.k8s.io/gateway-name label). Values must be
	// JSON-representable (strings, numbers, booleans, maps, slices).
	NodeMetadata map[string]any
	// Resources is the explicit resource-name subscription. Empty means the
	// wildcard subscription ("*"), which by xDS spec only Listener and
	// Cluster honor everywhere: spec-conforming servers (istiod) serve
	// Endpoint and Route resources exclusively by name.
	Resources []string
	// TLS enables mutual TLS towards the upstream (management servers such
	// as envoy-gateway serve xDS over mTLS only). Nil means plaintext.
	TLS *TLSConfig
	// Runtime is the engine runtime used to create a publisher. Required.
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

// TLSConfig carries the mTLS client material as file paths.
type TLSConfig struct {
	// CertFile and KeyFile are the client certificate and key.
	CertFile string
	KeyFile  string
	// CAFile is the CA bundle that signed the server certificate.
	CAFile string
	// ServerName overrides SNI/hostname verification (the server certificate
	// is usually issued for a service DNS name, not the dial address).
	ServerName string
}

// ListerConfig configures a Lister.
type ListerConfig = ProducerConfig

// WatcherConfig configures a Watcher.
type WatcherConfig = ProducerConfig

// producerBase holds the state shared by the ingest producers: the publisher,
// the resolved xDS type, and the upstream dial target.
type producerBase struct {
	*dbspruntime.BaseProducer

	entry     typeEntry
	address   string
	node      *corev3.Node
	topic     string
	typ       string
	tlsCfg    *TLSConfig
	resources []string
	log       logr.Logger
}

// newProducerBase resolves the xDS type against the registry and wires the
// publisher.
func newProducerBase(cfg ProducerConfig, componentType string) (*producerBase, error) {
	entry, ok := lookupType(cfg.Type)
	if !ok {
		return nil, fmt.Errorf("xds: unknown type %q (known: %v)", cfg.Type, KnownTypes())
	}

	node := cfg.Node
	if node == "" {
		node = defaultNode
	}
	var nodeMeta *structpb.Struct
	if len(cfg.NodeMetadata) > 0 {
		var err error
		nodeMeta, err = structpb.NewStruct(cfg.NodeMetadata)
		if err != nil {
			return nil, fmt.Errorf("xds: node metadata: %w", err)
		}
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName(componentType).WithValues("type", cfg.Type, "topic", cfg.InputName, "upstream", cfg.Address)

	base, err := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          cfg.Name,
		Publisher:     cfg.Runtime.NewPublisher(),
		ErrorReporter: cfg.Runtime,
		Logger:        log,
		Topics:        []string{cfg.InputName},
	})
	if err != nil {
		return nil, err
	}

	return &producerBase{
		BaseProducer: base,
		entry:        entry,
		address:      cfg.Address,
		node:         &corev3.Node{Id: node, Cluster: cfg.NodeCluster, Metadata: nodeMeta},
		topic:        cfg.InputName,
		typ:          cfg.Type,
		tlsCfg:       cfg.TLS,
		resources:    append([]string(nil), cfg.Resources...),
		log:          log,
	}, nil
}

// subscription returns the resource names for the initial subscription
// request: the explicit names when configured, the spec's wildcard resource
// name otherwise (the legacy empty-list wildcard is deprecated, and
// go-control-plane's delta server does not honor it).
func (p *producerBase) subscription() []string {
	if len(p.resources) > 0 {
		return p.resources
	}
	return []string{"*"}
}

// dial opens a gRPC connection to the upstream xDS server: plaintext by
// default, mutual TLS when the producer is configured with client material.
func (p *producerBase) dial() (*grpc.ClientConn, error) {
	creds := insecure.NewCredentials()
	if p.tlsCfg != nil {
		cert, err := tls.LoadX509KeyPair(p.tlsCfg.CertFile, p.tlsCfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("xds: load client cert: %w", err)
		}
		pool := x509.NewCertPool()
		ca, err := os.ReadFile(p.tlsCfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("xds: load CA: %w", err)
		}
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("xds: CA bundle %q contains no certificates", p.tlsCfg.CAFile)
		}
		creds = credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			ServerName:   p.tlsCfg.ServerName,
			MinVersion:   tls.VersionTLS12,
		})
	}
	conn, err := grpc.NewClient(p.address, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("xds: dial %q: %w", p.address, err)
	}
	return conn, nil
}

// emit publishes a Z-set of documents to the producer's input topic.
func (p *producerBase) emit(zs zset.ZSet) error {
	return p.Publish(dbspruntime.Event{Name: p.topic, Data: zs})
}
