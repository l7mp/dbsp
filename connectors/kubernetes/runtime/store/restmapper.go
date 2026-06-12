package store

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/restmapper"
)

var _ meta.RESTMapper = &CompositeRESTMapper{}

// CompositeRESTMapper implements meta.RESTMapper by routing view groups to ViewRESTMapper and
// native groups to native RESTMapper.
type CompositeRESTMapper struct {
	viewMapper   *ViewRESTMapper
	nativeMapper meta.RESTMapper
	discovery    discovery.DiscoveryInterface
}

// NewCompositeRESTMapper creates a new composite REST mapper.
func NewCompositeRESTMapper(compositeDiscovery discovery.DiscoveryInterface) *CompositeRESTMapper {
	// Native lookups go through a lazy mapper: running discovery eagerly here
	// would freeze an empty snapshot whenever the API server is not yet
	// reachable at process startup, permanently failing every native lookup.
	var nativeMapper meta.RESTMapper
	if compositeDiscovery != nil {
		nativeMapper = newLazyNativeMapper(compositeDiscovery)
	}

	// Create view RESTMapper
	viewMapper := NewViewRESTMapper()

	return &CompositeRESTMapper{
		viewMapper:   viewMapper,
		nativeMapper: nativeMapper,
		discovery:    compositeDiscovery,
	}
}

// KindFor returns the Kind for the given resource.
func (m *CompositeRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if m.isViewGroup(resource.Group) {
		return m.viewMapper.KindFor(resource)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.KindFor(resource)
	}

	return schema.GroupVersionKind{}, fmt.Errorf("no RESTMapper available for resource %s", resource)
}

// KindsFor returns all Kinds for the given resource.
func (m *CompositeRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	if m.isViewGroup(resource.Group) {
		return m.viewMapper.KindsFor(resource)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.KindsFor(resource)
	}

	return nil, fmt.Errorf("no RESTMapper available for resource %s", resource)
}

// ResourceFor returns the Resource for the given input.
func (m *CompositeRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	if m.isViewGroup(input.Group) {
		return m.viewMapper.ResourceFor(input)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.ResourceFor(input)
	}

	return schema.GroupVersionResource{}, fmt.Errorf("no RESTMapper available for resource %s", input)
}

// ResourcesFor returns all Resources for the given input.
func (m *CompositeRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	if m.isViewGroup(input.Group) {
		return m.viewMapper.ResourcesFor(input)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.ResourcesFor(input)
	}

	return nil, fmt.Errorf("no RESTMapper available for resource %s", input)
}

// RESTMapping returns the RESTMapping for the given GroupKind.
func (m *CompositeRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if m.isViewGroup(gk.Group) {
		return m.viewMapper.RESTMapping(gk, versions...)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.RESTMapping(gk, versions...)
	}

	return nil, fmt.Errorf("no RESTMapper available for GroupKind %s", gk)
}

// RESTMappings returns all RESTMappings for the given GroupKind.
func (m *CompositeRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	if m.isViewGroup(gk.Group) {
		return m.viewMapper.RESTMappings(gk, versions...)
	}

	if m.nativeMapper != nil {
		return m.nativeMapper.RESTMappings(gk, versions...)
	}

	return nil, fmt.Errorf("no RESTMapper available for GroupKind %s", gk)
}

// ResourceSingularizer returns the singular form of the resource.
func (m *CompositeRESTMapper) ResourceSingularizer(resource string) (string, error) {
	// For views, singular == plural (both lowercase kind)
	// For native resources, delegate to native mapper
	if m.nativeMapper != nil {
		singular, err := m.nativeMapper.ResourceSingularizer(resource)
		if err == nil {
			return singular, nil
		}
	}

	// Default: return as-is (works for views)
	return resource, nil
}

// isViewGroup checks if a group is a view group.
func (m *CompositeRESTMapper) isViewGroup(group string) bool {
	if cd, ok := m.discovery.(*CompositeDiscoveryClient); ok {
		return cd.IsViewGroup(group)
	}
	// Fallback check
	return group == "view.dcontroller.io"
}

// defaultMapperReloadInterval rate-limits miss-triggered re-discovery.
const defaultMapperReloadInterval = 5 * time.Second

// lazyNativeMapper is a meta.RESTMapper that builds its discovery-based
// delegate on first use and rebuilds it on lookup misses. An unreachable API
// server surfaces as a lookup error and the next lookup retries discovery, so
// late API server startup and CRDs registered after process start both
// resolve without a restart.
type lazyNativeMapper struct {
	discovery      discovery.DiscoveryInterface
	reloadInterval time.Duration

	mu         sync.Mutex
	delegate   meta.RESTMapper
	generation int
	lastLoad   time.Time
}

var _ meta.RESTMapper = &lazyNativeMapper{}

func newLazyNativeMapper(d discovery.DiscoveryInterface) *lazyNativeMapper {
	return &lazyNativeMapper{discovery: d, reloadInterval: defaultMapperReloadInterval}
}

// get returns the delegate and its load generation, running discovery if no
// delegate has been built yet.
func (l *lazyNativeMapper) get() (meta.RESTMapper, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.delegate != nil {
		return l.delegate, l.generation, nil
	}
	return l.loadLocked()
}

// reload re-runs discovery unless a successful load happened within the
// reload interval, in which case the current delegate is returned as is.
func (l *lazyNativeMapper) reload() (meta.RESTMapper, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.delegate != nil && time.Since(l.lastLoad) < l.reloadInterval {
		return l.delegate, l.generation, nil
	}
	return l.loadLocked()
}

func (l *lazyNativeMapper) loadLocked() (meta.RESTMapper, int, error) {
	groupResources, err := restmapper.GetAPIGroupResources(l.discovery)
	if err != nil {
		return nil, l.generation, fmt.Errorf("RESTMapper discovery: %w", err)
	}
	l.delegate = restmapper.NewDiscoveryRESTMapper(groupResources)
	l.generation++
	l.lastLoad = time.Now()
	return l.delegate, l.generation, nil
}

// lookup runs fn against the delegate; on a no-match error it re-runs
// discovery (rate-limited) and retries once, so resources registered since
// the last discovery resolve without waiting for a process restart.
func lookup[T any](l *lazyNativeMapper, fn func(meta.RESTMapper) (T, error)) (T, error) {
	m, gen, err := l.get()
	if err != nil {
		var zero T
		return zero, err
	}

	res, err := fn(m)
	if err == nil || !meta.IsNoMatchError(err) {
		return res, err
	}

	refreshed, rgen, reloadErr := l.reload()
	if reloadErr != nil || rgen == gen {
		return res, err
	}

	return fn(refreshed)
}

func (l *lazyNativeMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return lookup(l, func(m meta.RESTMapper) (schema.GroupVersionKind, error) { return m.KindFor(resource) })
}

func (l *lazyNativeMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return lookup(l, func(m meta.RESTMapper) ([]schema.GroupVersionKind, error) { return m.KindsFor(resource) })
}

func (l *lazyNativeMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return lookup(l, func(m meta.RESTMapper) (schema.GroupVersionResource, error) { return m.ResourceFor(input) })
}

func (l *lazyNativeMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return lookup(l, func(m meta.RESTMapper) ([]schema.GroupVersionResource, error) { return m.ResourcesFor(input) })
}

func (l *lazyNativeMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	return lookup(l, func(m meta.RESTMapper) (*meta.RESTMapping, error) { return m.RESTMapping(gk, versions...) })
}

func (l *lazyNativeMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	return lookup(l, func(m meta.RESTMapper) ([]*meta.RESTMapping, error) { return m.RESTMappings(gk, versions...) })
}

func (l *lazyNativeMapper) ResourceSingularizer(resource string) (string, error) {
	return lookup(l, func(m meta.RESTMapper) (string, error) { return m.ResourceSingularizer(resource) })
}
