package xds

import (
	"context"
	"errors"
	"fmt"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// testClient is a minimal in-process ADS client over the generated gRPC stubs.
// It asserts what a Server actually serves, without a real Envoy: it dials the
// Server's loopback Address and speaks both the State-of-the-World and the Delta
// variants of the Aggregated Discovery Service. go-control-plane ships no
// importable client fake, so this mirrors the small fake Istio uses internally.
type testClient struct {
	conn *grpc.ClientConn
	ads  discoveryv3.AggregatedDiscoveryServiceClient
}

// testNode identifies the client to the server; LinearCache is node-agnostic, so
// the value is immaterial, but a node is part of a well-formed request.
var testNode = &corev3.Node{Id: "test"}

// dialTestClient connects to an xDS Server at addr over an insecure loopback
// channel.
func dialTestClient(addr string) (*testClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &testClient{conn: conn, ads: discoveryv3.NewAggregatedDiscoveryServiceClient(conn)}, nil
}

// Close releases the underlying gRPC connection.
func (c *testClient) Close() error { return c.conn.Close() }

// decodeAny unmarshals an xDS resource carried as an Any into its concrete
// proto, keyed by the resource's self-assigned name. The proto types must be
// linked into the test binary for the registry lookup to resolve.
func decodeAny(any *anypb.Any) (string, proto.Message, error) {
	msg, err := any.UnmarshalNew()
	if err != nil {
		return "", nil, fmt.Errorf("unmarshal resource: %w", err)
	}
	return cache.GetResourceName(msg), msg, nil
}

// fetchSotW opens a fresh SotW ADS stream, requests one type URL (names empty
// means a wildcard subscription), and returns the resources of the first
// response keyed by name. A held-open watch (an empty cache that the server does
// not answer immediately) surfaces as ctx expiry, reported as an empty result so
// callers can poll with Eventually.
func (c *testClient) fetchSotW(ctx context.Context, typeURL string, names []string) (map[string]proto.Message, error) {
	stream, err := c.ads.StreamAggregatedResources(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&discoveryv3.DiscoveryRequest{
		Node:          testNode,
		TypeUrl:       typeURL,
		ResourceNames: names,
	}); err != nil {
		return nil, ignoreCtx(ctx, err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, ignoreCtx(ctx, err)
	}

	out := map[string]proto.Message{}
	for _, any := range resp.GetResources() {
		name, msg, err := decodeAny(any)
		if err != nil {
			return nil, err
		}
		out[name] = msg
	}
	return out, nil
}

// fetchDelta opens a fresh Delta ADS stream, subscribes to one type URL (names
// empty means a wildcard subscription), and returns the resources of the first
// response keyed by name. As with fetchSotW, ctx expiry yields an empty result.
func (c *testClient) fetchDelta(ctx context.Context, typeURL string, names []string) (map[string]proto.Message, error) {
	stream, err := c.ads.DeltaAggregatedResources(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&discoveryv3.DeltaDiscoveryRequest{
		Node:                   testNode,
		TypeUrl:                typeURL,
		ResourceNamesSubscribe: names,
	}); err != nil {
		return nil, ignoreCtx(ctx, err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, ignoreCtx(ctx, err)
	}

	out := map[string]proto.Message{}
	for _, r := range resp.GetResources() {
		name, msg, err := decodeAny(r.GetResource())
		if err != nil {
			return nil, err
		}
		out[name] = msg
	}
	return out, nil
}

// ignoreCtx collapses a context-cancellation error into a nil error (signalling
// "nothing served yet"); other errors pass through unchanged.
func ignoreCtx(ctx context.Context, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return nil //nolint:nilerr // cancellation means nothing served yet, not a failure
	}
	return err
}
