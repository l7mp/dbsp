package xds

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/spec"
)

// Group is the API group routing bindings to the xds connector.
const Group = "xds.connector.dcontroller.io"

// kinds maps the serialized resource kinds to xDS type shorthands.
var kinds = map[string]string{
	"Listener":              "lds",
	"RouteConfiguration":    "rds",
	"Cluster":               "cds",
	"ClusterLoadAssignment": "eds",
}

// Env is the host environment the factory closes over. Server resolves
// host-owned egress servers by name (nil when the host runs none): a
// host-started server is shared and outlives the loaded runtimes bound
// to it.
type Env struct {
	Server func(name string) (*Server, bool)
}

// NewFactory returns the xds connector's binding factory: delta-ADS
// ingest sources and egress targets served over an ADS server. A target
// binds a host-owned server by name when the host runs one; otherwise
// the first target naming an address creates a runtime-owned server that
// dies with the runtime.
func NewFactory(env Env) runtime.ConnectorFactory {
	var mu sync.Mutex
	servers := map[*runtime.Runtime]map[string]*Server{}

	ensureServer := func(rt *runtime.Runtime, params *json.RawMessage) (*Server, string, error) {
		var p struct {
			Server  string `json:"server"`
			Address string `json:"address"`
		}
		if params != nil {
			if err := json.Unmarshal(*params, &p); err != nil {
				return nil, "", fmt.Errorf("parameters: %w", err)
			}
		}
		name := rt.Name()
		if p.Server != "" {
			name = rt.Name() + "/" + p.Server
		}

		mu.Lock()
		defer mu.Unlock()
		if srv, ok := servers[rt][name]; ok {
			return srv, name, nil
		}
		if env.Server != nil {
			if srv, ok := env.Server(name); ok {
				return srv, name, nil
			}
		}
		if p.Address == "" {
			return nil, "", fmt.Errorf("xds server %q is not running and the target names no parameters.address to start it", name)
		}
		srv, err := NewServerFromSpec(ServerSpec{Name: name, Address: p.Address}, Deps{Runtime: rt, Logger: rt.Logger()})
		if err != nil {
			return nil, "", err
		}
		if err := rt.Add(&ownedServer{Server: srv, cleanup: func() {
			mu.Lock()
			defer mu.Unlock()
			delete(servers[rt], name)
			if len(servers[rt]) == 0 {
				delete(servers, rt)
			}
		}}); err != nil {
			return nil, "", err
		}
		if servers[rt] == nil {
			servers[rt] = map[string]*Server{}
		}
		servers[rt][name] = srv
		return srv, name, nil
	}

	return runtime.ConnectorFactory{
		Name:   "xds",
		Groups: []string{Group},
		NewSource: func(rt *runtime.Runtime, s *spec.Source, topic string) (runtime.Runnable, error) {
			if s.Type != "" && s.Type != spec.Watcher {
				return nil, fmt.Errorf("unknown xds source type %q", s.Type)
			}
			typ, ok := kinds[s.Kind]
			if !ok {
				return nil, fmt.Errorf("unknown xds resource kind %q", s.Kind)
			}
			var ps ProducerSpec
			if s.Parameters != nil {
				if err := json.Unmarshal(*s.Parameters, &ps); err != nil {
					return nil, fmt.Errorf("parameters: %w", err)
				}
			}
			ps.Type = typ
			return NewProducerFromSpec(topic, ps, Deps{Runtime: rt, Logger: rt.Logger()})
		},
		NewTarget: func(rt *runtime.Runtime, t *spec.Target, topic string) (runtime.Runnable, error) {
			if t.Type != "" && t.Type != spec.Updater {
				return nil, fmt.Errorf("unknown xds target type %q", t.Type)
			}
			typ, ok := kinds[t.Kind]
			if !ok {
				return nil, fmt.Errorf("unknown xds resource kind %q", t.Kind)
			}
			srv, name, err := ensureServer(rt, t.Parameters)
			if err != nil {
				return nil, err
			}
			// The wire mode against the remote (state-of-the-world
			// SetResources vs incremental UpdateResources) is connector
			// residue and rides the parameters.
			var tp struct {
				Level bool `json:"level"`
			}
			if t.Parameters != nil {
				if err := json.Unmarshal(*t.Parameters, &tp); err != nil {
					return nil, fmt.Errorf("parameters: %w", err)
				}
			}
			return NewConsumerFromSpec(srv, rt, topic, ConsumerSpec{Type: typ, Server: name, Level: tp.Level}, rt.Logger())
		},
	}
}

// ownedServer runs the runtime-owned ADS server and drops it from the
// factory's per-runtime index when it stops.
type ownedServer struct {
	*Server
	cleanup func()
}

func (o *ownedServer) Start(ctx context.Context) error {
	defer o.cleanup()
	return o.Server.Start(ctx)
}
