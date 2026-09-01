package runtime

import (
	"fmt"
	"strings"
	"sync"

	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/spec"
)

// ConnectorFactory describes one binding provider, routing serialized
// source and target bindings to the connector that implements them:
// plain data plus constructors, closed over the connector's own
// environment. The runtime stays connector-agnostic: assembly routes a
// binding by the resource group and hands the spec fragment to the
// factory, which owns its validation and construction; everything a
// factory builds is a Runnable owned by the runtime it is added to.
type ConnectorFactory struct {
	// Name identifies the factory in the registry and is the registrant
	// of its expression operators.
	Name string
	// Groups are the exact API groups routed to this factory.
	Groups []string
	// Resources marks the factory that takes plain resource references:
	// every group that is not a connector group (an absent group, a
	// Kubernetes API group, a view group). At most one factory may claim
	// this role.
	Resources bool
	// Init runs once per runtime load, before any binding is built, and
	// only when the spec references the factory.
	Init func(rt *Runtime, op *spec.RuntimeSpec) error
	// NewSource builds one source binding publishing to topic.
	NewSource func(rt *Runtime, s *spec.Source, topic string) (Runnable, error)
	// NewTarget builds one target binding consuming from topic.
	NewTarget func(rt *Runtime, t *spec.Target, topic string) (Runnable, error)
	// Expressions are the operators the factory contributes, bound into
	// the expression registry under the factory's name at Register time.
	Expressions map[string]dbspexpr.CallbackFunc
}

// ConnectorRegistry routes binding groups to registered factories. The
// zero registry is not usable; create one with NewConnectorRegistry.
type ConnectorRegistry struct {
	mu        sync.Mutex
	names     map[string]bool
	byGroup   map[string]*ConnectorFactory
	resources *ConnectorFactory
}

// NewConnectorRegistry creates an empty factory registry.
func NewConnectorRegistry() *ConnectorRegistry {
	return &ConnectorRegistry{names: map[string]bool{}, byGroup: map[string]*ConnectorFactory{}}
}

// Register adds a factory to the registry. Registering the same factory
// name again is a no-op, so hosts may register their connectors
// unconditionally.
func (registry *ConnectorRegistry) Register(f ConnectorFactory) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if f.Name == "" {
		return fmt.Errorf("connector: factory requires a name")
	}
	if registry.names[f.Name] {
		return nil
	}
	if f.Resources && registry.resources != nil {
		return fmt.Errorf("connector: %q claims plain resource references, already claimed by %q",
			f.Name, registry.resources.Name)
	}
	for _, g := range f.Groups {
		if have, ok := registry.byGroup[g]; ok {
			return fmt.Errorf("connector: group %q already registered by %q", g, have.Name)
		}
	}

	held := f
	registry.names[f.Name] = true
	for _, g := range held.Groups {
		registry.byGroup[g] = &held
	}
	if held.Resources {
		registry.resources = &held
	}
	for name, fn := range held.Expressions {
		if _, bound := dbspexpr.CallbackRegistrant(name); bound {
			continue
		}
		if err := dbspexpr.RegisterCallback(name, held.Name, fn); err != nil {
			return fmt.Errorf("connector: %q expression %s: %w", held.Name, name, err)
		}
	}
	return nil
}

// Names lists the registered factory names.
func (registry *ConnectorRegistry) Names() []string {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	out := make([]string, 0, len(registry.names))
	for name := range registry.names {
		out = append(out, name)
	}
	return out
}

// factoryFor routes a resource reference: an exact connector-group match
// wins, everything else is a plain resource reference.
func (registry *ConnectorRegistry) factoryFor(r spec.Resource) (*ConnectorFactory, error) {
	group := ""
	if r.Group != nil {
		group = strings.TrimSpace(*r.Group)
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if f, ok := registry.byGroup[group]; ok {
		return f, nil
	}
	if registry.resources == nil {
		return nil, fmt.Errorf("no connector for group %q", group)
	}
	return registry.resources, nil
}
