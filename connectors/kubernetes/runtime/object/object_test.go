package object

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Object")
}

var _ = Describe("Object", func() {
	It("deepequal", func() {
		obj1 := NewViewObject("test", "view1")
		obj2 := NewViewObject("test", "view2")

		Expect(DeepEqual(obj1, obj2)).To(BeFalse())
		Expect(DeepEqual(obj1, obj1)).To(BeTrue())
		Expect(DeepEqual(obj2, obj2)).To(BeTrue())
	})

	It("get-operator", func() {
		obj := NewViewObject("test", "view")
		op := GetOperator(obj)
		Expect(op).To(Equal("test"))
	})

	It("setcontent", func() {
		obj := NewViewObject("test", "view")
		SetContent(obj, map[string]any{"a": "x"})
		Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "test.view.dcontroller.io/v1alpha1",
			"kind":       "view",
			"a":          "x",
		}))
	})

	It("setname 1", func() {
		obj := NewViewObject("test", "view")
		SetContent(obj, map[string]any{"a": "x"})
		SetName(obj, "ns", "obj")

		Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "test.view.dcontroller.io/v1alpha1",
			"kind":       "view",
			"metadata": map[string]any{
				"namespace": "ns",
				"name":      "obj",
			},
			"a": "x",
		}))
	})

	It("setname 2", func() {
		obj := NewViewObject("test", "view")
		SetName(obj, "ns", "obj")
		SetContent(obj, map[string]any{"a": "x"})

		Expect(obj.UnstructuredContent()).To(Equal(map[string]any{
			"apiVersion": "test.view.dcontroller.io/v1alpha1",
			"kind":       "view",
			"metadata": map[string]any{
				"namespace": "ns",
				"name":      "obj",
			},
			"a": "x",
		}))
	})

	It("dump strips noisy metadata", func() {
		obj := New()
		obj.SetUnstructuredContent(map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":              "cm1",
				"namespace":         "default",
				"resourceVersion":   "123",
				"uid":               "uid-1",
				"managedFields":     []any{map[string]any{"manager": "x"}},
				"creationTimestamp": "now",
				"annotations": map[string]any{
					"kubectl.kubernetes.io/last-applied-configuration": "...",
					"app": "demo",
				},
			},
			"data": map[string]any{"k": "v"},
		})

		dump := Dump(obj)
		Expect(dump).To(ContainSubstring(`"name":"cm1"`))
		Expect(dump).To(ContainSubstring(`"namespace":"default"`))
		Expect(dump).To(ContainSubstring(`"resourceVersion":"123"`))
		Expect(dump).To(ContainSubstring(`"app":"demo"`))
		Expect(dump).NotTo(ContainSubstring("managedFields"))
		Expect(dump).NotTo(ContainSubstring("creationTimestamp"))
		Expect(dump).NotTo(ContainSubstring("last-applied-configuration"))
	})
})
