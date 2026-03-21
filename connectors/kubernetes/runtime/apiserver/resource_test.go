package apiserver

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("API server resources", func() {
	It("derives list GVK from object GVK", func() {
		gvk := schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "Thing"}
		list := listGVK(gvk)
		Expect(list.Kind).To(Equal("ThingList"))
		Expect(list.Group).To(Equal("g"))
		Expect(list.Version).To(Equal("v1"))
	})

	It("finds API resource metadata for view kinds", func() {
		compositeClient, _, err := newTestStoreComponents()
		Expect(err).NotTo(HaveOccurred())
		config, err := NewDefaultConfig("127.0.0.1", 0, compositeClient, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		config.EnableOpenAPI = false

		s, err := NewAPIServer(config)
		Expect(err).NotTo(HaveOccurred())

		gvk := viewv1a1.GroupVersionKind("res", "Sample")
		r, err := s.findAPIResource(gvk)
		Expect(err).NotTo(HaveOccurred())
		Expect(r.APIResource).NotTo(BeNil())
		Expect(r.APIResource.Name).To(Equal("sample"))
		Expect(r.APIResource.Kind).To(Equal("Sample"))
		Expect(r.APIResource.Namespaced).To(BeTrue())
		Expect(r.HasStatus).To(BeTrue())
	})
})
