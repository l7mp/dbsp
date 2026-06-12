package store

import (
	"errors"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// flakyDiscovery is a DiscoveryInterface stub whose group/resource listing
// fails until it is armed, simulating an API server that is not yet
// reachable at process startup. Resources can be extended later to simulate
// CRDs registered after the first discovery run.
type flakyDiscovery struct {
	discovery.DiscoveryInterface

	mu         sync.Mutex
	available  bool
	calls      int
	groups     []*metav1.APIGroup
	resources  []*metav1.APIResourceList
	partialErr error
}

func (f *flakyDiscovery) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if !f.available {
		return nil, nil, errors.New("connection refused")
	}
	// A partial failure returns the discoverable groups alongside the error,
	// as real discovery does for broken aggregated APIServices.
	return f.groups, f.resources, f.partialErr
}

func (f *flakyDiscovery) setAvailable(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.available = v
}

func (f *flakyDiscovery) addGroup(group, version, kind, plural string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	gv := schema.GroupVersion{Group: group, Version: version}
	f.groups = append(f.groups, &metav1.APIGroup{
		Name:     group,
		Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: gv.String(), Version: version}},
		PreferredVersion: metav1.GroupVersionForDiscovery{
			GroupVersion: gv.String(), Version: version,
		},
	})
	f.resources = append(f.resources, &metav1.APIResourceList{
		GroupVersion: gv.String(),
		APIResources: []metav1.APIResource{
			{Name: plural, SingularName: kind, Namespaced: true, Kind: kind},
		},
	})
}

func (f *flakyDiscovery) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var _ = Describe("LazyNativeMapper", func() {
	var (
		fake   *flakyDiscovery
		mapper *lazyNativeMapper
	)

	BeforeEach(func() {
		fake = &flakyDiscovery{}
		fake.addGroup("apps", "v1", "Deployment", "deployments")
		mapper = newLazyNativeMapper(fake)
		mapper.reloadInterval = 0 // disable rate limiting in tests
	})

	Describe("startup race", func() {
		It("should fail lookups while discovery is unavailable and recover once it is", func() {
			fake.setAvailable(false)

			_, err := mapper.KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("connection refused"))

			// The API server comes up later: the next lookup must succeed
			// without any reconstruction of the mapper.
			fake.setAvailable(true)

			gvk, err := mapper.KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
			Expect(err).NotTo(HaveOccurred())
			Expect(gvk).To(Equal(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}))
		})

		It("should retry discovery on every lookup until it succeeds", func() {
			fake.setAvailable(false)

			for range 3 {
				_, err := mapper.KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
				Expect(err).To(HaveOccurred())
			}
			Expect(fake.callCount()).To(Equal(3))
		})
	})

	Describe("late-registered resources", func() {
		It("should resolve a group registered after the first discovery run", func() {
			fake.setAvailable(true)

			// Prime the delegate with the initial group set.
			_, err := mapper.KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
			Expect(err).NotTo(HaveOccurred())

			// A CRD is registered after the snapshot was taken.
			fake.addGroup("example.org", "v1alpha1", "Widget", "widgets")

			gvk, err := mapper.KindFor(schema.GroupVersionResource{Group: "example.org", Version: "v1alpha1", Resource: "widgets"})
			Expect(err).NotTo(HaveOccurred())
			Expect(gvk).To(Equal(schema.GroupVersionKind{Group: "example.org", Version: "v1alpha1", Kind: "Widget"}))
		})

		It("should return the no-match error when the resource genuinely does not exist", func() {
			fake.setAvailable(true)

			_, err := mapper.KindFor(schema.GroupVersionResource{Group: "nonexistent.org", Version: "v1", Resource: "ghosts"})
			Expect(err).To(HaveOccurred())
		})

		It("should not re-run discovery on misses within the reload interval", func() {
			fake.setAvailable(true)
			mapper.reloadInterval = defaultMapperReloadInterval

			_, err := mapper.KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
			Expect(err).NotTo(HaveOccurred())
			Expect(fake.callCount()).To(Equal(1))

			_, err = mapper.KindFor(schema.GroupVersionResource{Group: "nonexistent.org", Version: "v1", Resource: "ghosts"})
			Expect(err).To(HaveOccurred())
			Expect(fake.callCount()).To(Equal(1), "miss within the reload interval must not trigger re-discovery")
		})
	})

	Describe("composite integration", func() {
		It("should serve native lookups through CompositeRESTMapper after a slow API server start", func() {
			fake.setAvailable(false)
			composite := NewCompositeRESTMapper(NewCompositeDiscoveryClient(fake))

			_, err := composite.KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
			Expect(err).To(HaveOccurred())

			fake.setAvailable(true)

			gvk, err := composite.KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
			Expect(err).NotTo(HaveOccurred())
			Expect(gvk.Kind).To(Equal("Deployment"))
		})

		It("should resolve healthy groups when one aggregated APIService is broken", func() {
			fake.setAvailable(true)
			fake.partialErr = &discovery.ErrGroupDiscoveryFailed{
				Groups: map[schema.GroupVersion]error{
					{Group: "metrics.k8s.io", Version: "v1beta1"}: errors.New("the server is currently unable to handle the request"),
				},
			}
			composite := NewCompositeRESTMapper(NewCompositeDiscoveryClient(fake))

			gvk, err := composite.KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
			Expect(err).NotTo(HaveOccurred(),
				"a single broken APIService must not fail lookups for healthy groups")
			Expect(gvk.Kind).To(Equal("Deployment"))
		})
	})
})
