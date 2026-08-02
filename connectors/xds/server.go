package xds

import (
	"context"
	"errors"
	"fmt"
	"net"

	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// defaultServerName is the runtime component name used when ServerConfig.Name is
// empty.
const defaultServerName = "xds"

// ServerConfig configures an xDS Server.
type ServerConfig struct {
	// Name is the unique runtime component name. Defaults to "xds".
	Name string
	// Address is the gRPC listen address, e.g. ":18000". ":0" lets the OS assign
	// a port, readable afterwards via Address.
	Address string
	// Runtime is the engine runtime that Consumers subscribe through. Required.
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

// Server serves xDS resources to Envoy over one gRPC endpoint. It holds one
// LinearCache per xDS type, multiplexed by type URL, and serves them over the
// Aggregated Discovery Service (ADS), in both its State-of-the-World and Delta
// variants. Updaters write resources into the caches. Server is a runtime
// Runnable: Start serves until its context is cancelled, then stops gracefully.
type Server struct {
	name string
	rt   *dbspruntime.Runtime
	log  logr.Logger

	lis  net.Listener
	grpc *grpc.Server

	caches map[string]*cachev3.LinearCache // keyed by xDS type URL

	srvCancel context.CancelFunc
}

// NewServer builds a Server listening on cfg.Address. The listener opens
// immediately, so Address reports the resolved address even for ":0".
func NewServer(cfg ServerConfig) (*Server, error) {
	name := cfg.Name
	if name == "" {
		name = defaultServerName
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("xds").WithValues("server", name)

	addr := cfg.Address
	if addr == "" {
		addr = ":0"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("xds: listen on %q: %w", addr, err)
	}

	// One LinearCache per xDS type, the MuxCache routing requests by type URL.
	caches := map[string]*cachev3.LinearCache{}
	muxed := map[string]cachev3.Cache{}
	for _, entry := range registry {
		if _, ok := caches[entry.url]; ok {
			continue
		}
		lc := cachev3.NewLinearCache(entry.url)
		caches[entry.url] = lc
		muxed[entry.url] = lc
	}
	mux := &cachev3.MuxCache{
		Classify:      func(r *cachev3.Request) string { return r.GetTypeUrl() },
		ClassifyDelta: func(r *cachev3.DeltaRequest) string { return r.GetTypeUrl() },
		Caches:        muxed,
	}

	srvCtx, srvCancel := context.WithCancel(context.Background())
	xdsSrv := serverv3.NewServer(srvCtx, mux, nil)

	grpcSrv := grpc.NewServer()
	// ADS only: one ordered stream multiplexing every type. The per-type split
	// streams are deliberately not registered.
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcSrv, xdsSrv)

	return &Server{
		name:      name,
		rt:        cfg.Runtime,
		log:       log,
		lis:       lis,
		grpc:      grpcSrv,
		caches:    caches,
		srvCancel: srvCancel,
	}, nil
}

// Name returns the unique runtime component name.
func (s *Server) Name() string { return s.name }

// Address returns the resolved gRPC listen address.
func (s *Server) Address() string { return s.lis.Addr().String() }

// Start serves until ctx is cancelled, then closes open watches and gracefully
// stops the gRPC server.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.grpc.Serve(s.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		s.srvCancel()
		s.grpc.GracefulStop()
		return nil
	case err := <-errCh:
		s.srvCancel()
		return fmt.Errorf("xds: serve: %w", err)
	}
}
