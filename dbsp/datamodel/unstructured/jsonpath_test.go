package unstructured_test

import (
	"errors"

	"github.com/l7mp/dbsp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Unstructured JSONPath", func() {
	var doc *unstructured.Unstructured

	BeforeEach(func() {
		doc = unstructured.New(map[string]any{
			"metadata": map[string]any{
				"namespace": "default",
				"name":      "name",
				"annotations": map[string]any{
					"kubernetes.io/service-name":  "example",
					"kubernetes.io[service-name]": "weirdness",
				},
			},
			"spec": map[string]any{
				"a": int64(1),
				"b": map[string]any{"c": int64(2)},
				"x": []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
				"ports": []any{
					map[string]any{"name": "udp-ok", "protocol": "UDP", "port": int64(1)},
					map[string]any{"name": "tcp-ok", "protocol": "TCP", "port": int64(2)},
				},
			},
		}, nil)
	})

	It("gets values with dot and bracket JSONPath", func() {
		v, err := doc.GetField("$.metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("name"))

		v, err = doc.GetField(`$["metadata"]["namespace"]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("default"))
	})

	It("gets nested object and indexed array items", func() {
		v, err := doc.GetField("$.metadata")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(map[string]any{
			"namespace": "default",
			"name":      "name",
			"annotations": map[string]any{
				"kubernetes.io/service-name":  "example",
				"kubernetes.io[service-name]": "weirdness",
			},
		}))

		v, err = doc.GetField("$.spec.ports[1].port")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(2)))
	})

	It("supports filter JSONPath", func() {
		v, err := doc.GetField(`$.spec.ports[?(@.name == 'udp-ok')].protocol`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("UDP"))
	})

	It("supports escaped annotation keys", func() {
		v, err := doc.GetField(`$["metadata"]["annotations"]["kubernetes.io/service-name"]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("example"))

		v, err = doc.GetField(`$["metadata"]["annotations"]["kubernetes.io[service-name]"]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("weirdness"))
	})

	It("returns root object for $", func() {
		v, err := doc.GetField("$")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(map[string]any{
			"metadata": map[string]any{
				"namespace": "default",
				"name":      "name",
				"annotations": map[string]any{
					"kubernetes.io/service-name":  "example",
					"kubernetes.io[service-name]": "weirdness",
				},
			},
			"spec": map[string]any{
				"a": int64(1),
				"b": map[string]any{"c": int64(2)},
				"x": []any{int64(1), int64(2), int64(3), int64(4), int64(5)},
				"ports": []any{
					map[string]any{"name": "udp-ok", "protocol": "UDP", "port": int64(1)},
					map[string]any{"name": "tcp-ok", "protocol": "TCP", "port": int64(2)},
				},
			},
		}))
	})

	It("supports full bracket selector syntax", func() {
		v, err := doc.GetField(`$["spec"]["b"]["c"]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(2)))
	})

	It("returns list for multi-match selectors", func() {
		v, err := doc.GetField(`$.spec.ports[*].name`)
		Expect(err).NotTo(HaveOccurred())
		names, ok := v.([]any)
		Expect(ok).To(BeTrue())
		Expect(names).To(Equal([]any{"udp-ok", "tcp-ok"}))
	})

	It("returns list for multi-match scalar path", func() {
		v, err := doc.GetField(`$.spec.ports[?(@.protocol in ['UDP','TCP'])].port`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal([]any{int64(1), int64(2)}))
	})

	It("sets nested values and creates missing paths with JSONPath", func() {
		err := doc.SetField("$.spec.y", "aaa")
		Expect(err).NotTo(HaveOccurred())
		err = doc.SetField("$.spec.b.d", int64(12))
		Expect(err).NotTo(HaveOccurred())

		v, err := doc.GetField("$.spec.y")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("aaa"))

		v, err = doc.GetField("$.spec.b.d")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(12)))
	})

	It("sets array index with JSONPath", func() {
		err := doc.SetField("$.y[3]", int64(12))
		Expect(err).NotTo(HaveOccurred())

		v, err := doc.GetField("$.y")
		Expect(err).NotTo(HaveOccurred())
		arr, ok := v.([]any)
		Expect(ok).To(BeTrue())
		Expect(arr).To(HaveLen(4))
		Expect(arr[0]).To(BeNil())
		Expect(arr[1]).To(BeNil())
		Expect(arr[2]).To(BeNil())
		Expect(arr[3]).To(Equal(int64(12)))
	})

	It("rejects root path set with $", func() {
		err := doc.SetField("$", map[string]any{"a": int64(1)})
		Expect(err).To(HaveOccurred())
	})

	It("sets escaped annotation keys through bracket syntax", func() {
		err := doc.SetField(`$["metadata"]["annotations"]["kubernetes.io/service-name"]`, "changed")
		Expect(err).NotTo(HaveOccurred())
		v, err := doc.GetField(`$["metadata"]["annotations"]["kubernetes.io/service-name"]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("changed"))
	})

	It("supports dotted-path getter compatibility for nested values", func() {
		v, err := doc.GetField("spec.b.c")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(2)))
	})

	It("returns parse errors for invalid JSONPath", func() {
		_, err := doc.GetField("$[")
		Expect(err).To(HaveOccurred())

		err = doc.SetField("$[", int64(1))
		Expect(err).To(HaveOccurred())
	})

	It("returns ErrFieldNotFound for empty selector result", func() {
		_, err := doc.GetField(`$.spec.ports[?(@.name == 'nope')].protocol`)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, datamodel.ErrFieldNotFound)).To(BeTrue())
	})

	It("keeps dotted-path setter compatibility", func() {
		err := doc.SetField("metadata.name", "fixed")
		Expect(err).NotTo(HaveOccurred())

		v, err := doc.GetField("metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal("fixed"))
	})

	It("reports ErrFieldNotFound for missing JSONPath", func() {
		_, err := doc.GetField("$.spec.missing")
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, datamodel.ErrFieldNotFound)).To(BeTrue())
	})
})
