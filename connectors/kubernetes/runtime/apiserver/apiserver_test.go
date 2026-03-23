package apiserver

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
)

// newTestStoreComponents creates the minimal store components needed for apiserver tests.
func newTestStoreComponents() (compositeClient *store.CompositeClient, compositeDiscovery *store.CompositeDiscoveryClient, err error) {
	compositeDiscovery = store.NewCompositeDiscoveryClient(nil)
	compositeCache, err := store.NewCompositeCache(nil, store.CacheOptions{Logger: logr.Discard()})
	if err != nil {
		return nil, nil, err
	}
	compositeClient, err = store.NewCompositeClient(nil, compositeCache, store.ClientOptions{})
	if err != nil {
		return nil, nil, err
	}
	return compositeClient, compositeDiscovery, nil
}

var _ = Describe("API server", func() {
	It("starts and serves discovery", func() {
		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)

		compositeClient, compositeDiscovery, err := newTestStoreComponents()
		Expect(err).NotTo(HaveOccurred())

		config, err := NewDefaultConfig("127.0.0.1", 0, compositeClient, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		config.DiscoveryClient = compositeDiscovery
		config.EnableOpenAPI = false

		s, err := NewAPIServer(config)
		Expect(err).NotTo(HaveOccurred())

		gvk := viewv1a1.GroupVersionKind("test", "TestView")
		Expect(s.RegisterGVKs([]schema.GroupVersionKind{gvk})).To(Succeed())

		errCh := make(chan error, 1)
		go func() { errCh <- s.Start(ctx) }()

		Eventually(func() bool { return s.running }, 2*time.Second, 20*time.Millisecond).Should(BeTrue())

		addr := s.GetInsecureServerAddress()
		Expect(addr).NotTo(BeEmpty())
		Expect(addr).NotTo(Equal("<unknown>"))

		dc, err := discovery.NewDiscoveryClientForConfig(&rest.Config{Host: fmt.Sprintf("http://%s", addr)})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			groups, resources, err := dc.ServerGroupsAndResources()
			if err != nil || len(groups) == 0 {
				return false
			}
			for _, g := range groups {
				if g.Name != viewv1a1.Group("test") {
					continue
				}
				for _, rl := range resources {
					if rl.GroupVersion != gvk.GroupVersion().String() {
						continue
					}
					for _, r := range rl.APIResources {
						if r.Kind == gvk.Kind {
							return true
						}
					}
				}
			}
			return false
		}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))
	})

	It("registers and unregisters GVKs", func() {
		compositeClient, compositeDiscovery, err := newTestStoreComponents()
		Expect(err).NotTo(HaveOccurred())

		config, err := NewDefaultConfig("127.0.0.1", 0, compositeClient, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		config.DiscoveryClient = compositeDiscovery
		config.EnableOpenAPI = false

		s, err := NewAPIServer(config)
		Expect(err).NotTo(HaveOccurred())

		g1 := viewv1a1.GroupVersionKind("g1", "View1")
		g2 := viewv1a1.GroupVersionKind("g2", "View2")

		Expect(s.RegisterGVKs([]schema.GroupVersionKind{g1, g2})).To(Succeed())

		s.mu.RLock()
		_, ok1 := s.groupGVKs[g1.Group]
		_, ok2 := s.groupGVKs[g2.Group]
		s.mu.RUnlock()
		Expect(ok1).To(BeTrue())
		Expect(ok2).To(BeTrue())

		s.UnregisterGVKs([]schema.GroupVersionKind{g1, g2})

		s.mu.RLock()
		_, ok1 = s.groupGVKs[g1.Group]
		_, ok2 = s.groupGVKs[g2.Group]
		s.mu.RUnlock()
		Expect(ok1).To(BeFalse())
		Expect(ok2).To(BeFalse())
	})

	It("finds only view API resources", func() {
		compositeClient, compositeDiscovery, err := newTestStoreComponents()
		Expect(err).NotTo(HaveOccurred())

		config, err := NewDefaultConfig("127.0.0.1", 0, compositeClient, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		config.DiscoveryClient = compositeDiscovery
		config.EnableOpenAPI = false

		s, err := NewAPIServer(config)
		Expect(err).NotTo(HaveOccurred())

		_, err = s.findAPIResource(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"})
		Expect(err).To(HaveOccurred())

		gvk := viewv1a1.GroupVersionKind("t", "MyView")
		r, err := s.findAPIResource(gvk)
		Expect(err).NotTo(HaveOccurred())
		Expect(r.APIResource).NotTo(BeNil())
		Expect(r.APIResource.Kind).To(Equal("MyView"))
		Expect(r.HasStatus).To(BeTrue())
	})
})
