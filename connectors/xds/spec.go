package xds

import (
	"fmt"

	"github.com/go-logr/logr"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// The serialized configurations of the connector's verbs: the one wire
// form every frontend funnels through (the JavaScript xds.* options
// objects and a serialized controller's source/target parameters
// deserialize into these alike).

// ServerSpec configures an egress server.
type ServerSpec struct {
	Name    string `json:"name,omitempty"`
	Address string `json:"address"`
}

// ProducerSpec configures a delta-ADS ingest producer (level: true for
// state-of-the-world snapshots per event).
type ProducerSpec struct {
	Type         string         `json:"type"`
	Address      string         `json:"address"`
	Node         string         `json:"node,omitempty"`
	NodeCluster  string         `json:"nodeCluster,omitempty"`
	NodeMetadata map[string]any `json:"nodeMetadata,omitempty"`
	Resources    []string       `json:"resources,omitempty"`
	TLS          *TLSSpec       `json:"tls,omitempty"`
	Level        bool           `json:"level,omitempty"`
}

// TLSSpec is the serialized client TLS material of a producer.
type TLSSpec struct {
	Cert       string `json:"cert,omitempty"`
	Key        string `json:"key,omitempty"`
	CA         string `json:"ca,omitempty"`
	ServerName string `json:"serverName,omitempty"`
}

// ConsumerSpec configures an egress consumer bound to a server by name
// (level: true for state-of-the-world ownership of the type).
type ConsumerSpec struct {
	Type   string `json:"type"`
	Server string `json:"server,omitempty"`
	Level  bool   `json:"level,omitempty"`
}

// Deps carries the non-serializable runtime dependencies.
type Deps struct {
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

// NewServerFromSpec builds an egress server.
func NewServerFromSpec(spec ServerSpec, deps Deps) (*Server, error) {
	return NewServer(ServerConfig{
		Name:    spec.Name,
		Address: spec.Address,
		Runtime: deps.Runtime,
		Logger:  deps.Logger,
	})
}

// NewProducerFromSpec builds the ingest producer publishing to topic: a
// Watcher, or a Lister with level: true.
func NewProducerFromSpec(topic string, spec ProducerSpec, deps Deps) (dbspruntime.Runnable, error) {
	if spec.Address == "" {
		return nil, fmt.Errorf("empty address")
	}
	producerKind := "watcher"
	if spec.Level {
		producerKind = "lister"
	}
	name := fmt.Sprintf("xds-producer-%s-%s-%s", producerKind, topic, spec.Type)
	if spec.Node != "" {
		// Several clients may feed one topic (per-gateway scoped
		// upstreams); the node id keeps the component names unique.
		name = fmt.Sprintf("%s-%s", name, spec.Node)
	}
	cfg := ProducerConfig{
		Name:         name,
		InputName:    topic,
		Type:         spec.Type,
		Address:      spec.Address,
		Node:         spec.Node,
		NodeCluster:  spec.NodeCluster,
		NodeMetadata: spec.NodeMetadata,
		Resources:    spec.Resources,
		Runtime:      deps.Runtime,
		Logger:       deps.Logger,
	}
	if spec.TLS != nil {
		cfg.TLS = &TLSConfig{
			CertFile:   spec.TLS.Cert,
			KeyFile:    spec.TLS.Key,
			CAFile:     spec.TLS.CA,
			ServerName: spec.TLS.ServerName,
		}
	}
	if spec.Level {
		return NewLister(cfg)
	}
	return NewWatcher(cfg)
}

// NewConsumerFromSpec builds the egress consumer subscribed to topic on
// the given server: an Updater, or a Setter with level: true.
func NewConsumerFromSpec(srv *Server, rt *dbspruntime.Runtime, topic string, spec ConsumerSpec, logger logr.Logger) (dbspruntime.Runnable, error) {
	consumerKind := "updater"
	if spec.Level {
		consumerKind = "setter"
	}
	cfg := UpdaterConfig{
		Name:       fmt.Sprintf("xds-consumer-%s-%s-%s", consumerKind, topic, spec.Type),
		OutputName: topic,
		Type:       spec.Type,
		Logger:     logger,
		Runtime:    rt,
	}
	if spec.Level {
		return srv.Setter(cfg)
	}
	return srv.Updater(cfg)
}
