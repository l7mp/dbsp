package object

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DecodeSecretData", func() {
	DescribeTable("decodes secret data payload",
		func(input *corev1.Secret, expected map[string]string) {
			obj, err := NewViewObjectFromNativeObject("test", "Secret", input)
			Expect(err).NotTo(HaveOccurred())

			DecodeSecretData(obj)

			content := obj.UnstructuredContent()
			dataField, found := content["data"]
			if len(expected) > 0 {
				Expect(found).To(BeTrue())
			}

			if len(expected) == 0 {
				return
			}

			dataMap, ok := dataField.(map[string]any)
			Expect(ok).To(BeTrue())

			for key, expectedValue := range expected {
				actualValue, found := dataMap[key]
				Expect(found).To(BeTrue())
				actualStr, ok := actualValue.(string)
				Expect(ok).To(BeTrue())
				Expect(actualStr).To(Equal(expectedValue))
			}
		},
		Entry("multiple keys", &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "testsecret"},
			Type:       corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				"username": []byte("admin"),
				"password": []byte("s3cr3t"),
				"config":   []byte(`{"key":"value"}`),
			},
		}, map[string]string{"username": "admin", "password": "s3cr3t", "config": `{"key":"value"}`}),
		Entry("binary data", &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "binarysecret"},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"binarykey": {0x00, 0x01, 0x02, 0xFF}},
		}, map[string]string{"binarykey": "\x00\x01\x02\xff"}),
		Entry("empty data", &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "emptysecret"},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{},
		}, map[string]string{}),
	)

	It("does not modify non-secret objects", func() {
		cm := NewViewObject("test", "ConfigMap")
		SetName(cm, "default", "testconfigmap")
		SetContent(cm, map[string]any{"data": map[string]any{"key": "value"}})

		DecodeSecretData(cm)

		content := cm.UnstructuredContent()
		dataField, found := content["data"]
		Expect(found).To(BeTrue())

		dataMap, ok := dataField.(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(dataMap["key"]).To(Equal("value"))
	})

	It("handles nil objects", func() {
		DecodeSecretData(nil)
	})
})
