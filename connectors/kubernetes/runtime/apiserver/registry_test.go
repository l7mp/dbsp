package apiserver

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("API server registry", func() {
	It("registers GVK groups by API group", func() {
		compositeClient, _, err := newTestStoreComponents()
		Expect(err).NotTo(HaveOccurred())
		config, err := NewDefaultConfig("127.0.0.1", 0, compositeClient, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		config.EnableOpenAPI = false

		s, err := NewAPIServer(config)
		Expect(err).NotTo(HaveOccurred())

		g1 := viewv1a1.GroupVersionKind("reg1", "V1")
		g2 := viewv1a1.GroupVersionKind("reg2", "V2")
		Expect(s.RegisterGVKs([]schema.GroupVersionKind{g1, g2})).To(Succeed())

		s.mu.RLock()
		_, ok1 := s.groupGVKs[g1.Group]
		_, ok2 := s.groupGVKs[g2.Group]
		s.mu.RUnlock()
		Expect(ok1).To(BeTrue())
		Expect(ok2).To(BeTrue())
	})

	It("unregisters API groups idempotently", func() {
		compositeClient, _, err := newTestStoreComponents()
		Expect(err).NotTo(HaveOccurred())
		config, err := NewDefaultConfig("127.0.0.1", 0, compositeClient, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		config.EnableOpenAPI = false

		s, err := NewAPIServer(config)
		Expect(err).NotTo(HaveOccurred())

		group := viewv1a1.Group("idem")
		gvk := viewv1a1.GroupVersionKind("idem", "View")
		Expect(s.RegisterAPIGroup(group, []schema.GroupVersionKind{gvk})).To(Succeed())

		s.UnregisterAPIGroup(group)
		s.UnregisterAPIGroup(group)

		s.mu.RLock()
		_, ok := s.groupGVKs[group]
		s.mu.RUnlock()
		Expect(ok).To(BeFalse())
	})
})
