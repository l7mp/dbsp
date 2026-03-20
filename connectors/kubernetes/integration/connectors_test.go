package integration_test

import (
	"context"
	"fmt"
	"sort"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	"github.com/l7mp/dbsp/connectors/kubernetes/producer"
	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Kubernetes connectors over envtest", func() {
	ctx := context.Background()

	It("producer emits add, update, and delete deltas for ConfigMaps", func() {
		p, err := producer.New(producer.Config{
			Client:    suite.WatchClient,
			SourceGVK: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			InputName: "in",
			Namespace: suite.Namespace,
		})
		Expect(err).NotTo(HaveOccurred())

		ch := make(chan dbspruntime.Event, 8)
		p.SetPublisher(dbspruntime.PublishFunc(func(in dbspruntime.Event) error {
			ch <- in
			return nil
		}))

		startCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			defer GinkgoRecover()
			err := p.Start(startCtx)
			Expect(err).NotTo(HaveOccurred())
		}()

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "p-cm", Namespace: suite.Namespace},
			Data:       map[string]string{"k": "v1"},
		}
		Expect(suite.K8sClient.Create(ctx, cm)).To(Succeed())

		in := mustReceiveInput(ch)
		Expect(in.Name).To(Equal("in"))
		Expect(weights(in.Data)).To(Equal([]zset.Weight{1}))
		Expect(singleField(in.Data, "data", "k")).To(Equal("v1"))

		Eventually(func() error {
			obj := &corev1.ConfigMap{}
			if err := suite.K8sClient.Get(ctx, client.ObjectKeyFromObject(cm), obj); err != nil {
				return err
			}
			obj.Data["k"] = "v2"
			return suite.K8sClient.Update(ctx, obj)
		}, suite.Timeout, suite.Interval).Should(Succeed())

		in = mustReceiveInput(ch)
		Expect(weights(in.Data)).To(Equal([]zset.Weight{-1, 1}))
		Expect(allFieldValues(in.Data, "data", "k")).To(ConsistOf("v1", "v2"))

		Expect(suite.K8sClient.Delete(ctx, cm)).To(Succeed())

		in = mustReceiveInput(ch)
		Expect(weights(in.Data)).To(Equal([]zset.Weight{-1}))
		Expect(singleField(in.Data, "data", "k")).To(Equal("v2"))
	})

	It("updater writes and removes native Kubernetes objects", func() {
		u, err := consumer.NewUpdater(consumer.Config{
			Client:     suite.K8sClient,
			OutputName: "out",
			TargetGVK:  schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
		})
		Expect(err).NotTo(HaveOccurred())

		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "u-cm",
				"namespace": suite.Namespace,
			},
			"data": map[string]any{"a": "1"},
		}

		Expect(u.Consume(ctx, output("out", doc, 1))).To(Succeed())

		Eventually(func(g Gomega) {
			got := &corev1.ConfigMap{}
			err := suite.K8sClient.Get(ctx, client.ObjectKey{Namespace: suite.Namespace, Name: "u-cm"}, got)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got.Data["a"]).To(Equal("1"))
		}, suite.Timeout, suite.Interval).Should(Succeed())

		Expect(u.Consume(ctx, output("out", doc, -1))).To(Succeed())

		Eventually(func() bool {
			got := &corev1.ConfigMap{}
			err := suite.K8sClient.Get(ctx, client.ObjectKey{Namespace: suite.Namespace, Name: "u-cm"}, got)
			return client.IgnoreNotFound(err) != nil
		}, suite.Timeout, suite.Interval).Should(BeFalse())
	})

	It("patcher applies merge-style changes on native Kubernetes objects", func() {
		seed := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "p-deploy",
				"namespace": suite.Namespace,
			},
			"spec": map[string]any{
				"selector": map[string]any{
					"matchLabels": map[string]any{"app": "p-deploy"},
				},
				"template": map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{"app": "p-deploy"},
					},
					"spec": map[string]any{
						"containers": []any{map[string]any{
							"name":  "app",
							"image": "nginx:stable",
						}},
					},
				},
				"replicas": int64(1),
				"strategy": map[string]any{"type": "RollingUpdate"},
			},
		}}
		seed.SetGroupVersionKind(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"})
		Expect(suite.K8sClient.Create(ctx, seed)).To(Succeed())

		p, err := consumer.NewPatcher(consumer.Config{
			Client:     suite.K8sClient,
			OutputName: "out",
			TargetGVK:  schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
		})
		Expect(err).NotTo(HaveOccurred())

		patch := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "p-deploy",
				"namespace": suite.Namespace,
			},
			"spec": map[string]any{
				"replicas": int64(2),
			},
		}

		Expect(p.Consume(ctx, output("out", patch, 1))).To(Succeed())

		Eventually(func(g Gomega) {
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"})
			err := suite.K8sClient.Get(ctx, client.ObjectKey{Namespace: suite.Namespace, Name: "p-deploy"}, got)
			g.Expect(err).NotTo(HaveOccurred())
			replicas, ok, err := unstructured.NestedInt64(got.Object, "spec", "replicas")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue())
			g.Expect(replicas).To(Equal(int64(2)))
			strategy, ok, err := unstructured.NestedMap(got.Object, "spec", "strategy")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue())
			g.Expect(strategy).To(HaveKeyWithValue("type", "RollingUpdate"))
		}, suite.Timeout, suite.Interval).Should(Succeed())

		removeReplicas := map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"name":      "p-deploy",
				"namespace": suite.Namespace,
			},
			"spec": map[string]any{
				"replicas": int64(2),
			},
		}
		Expect(p.Consume(ctx, output("out", removeReplicas, -1))).To(Succeed())

		Eventually(func() bool {
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"})
			if err := suite.K8sClient.Get(ctx, client.ObjectKey{Namespace: suite.Namespace, Name: "p-deploy"}, got); err != nil {
				return false
			}
			replicas, ok, _ := unstructured.NestedInt64(got.Object, "spec", "replicas")
			if !ok {
				return true
			}
			return replicas == int64(1)
		}, suite.Timeout, suite.Interval).Should(BeTrue())
	})
})

func output(name string, doc map[string]any, w zset.Weight) dbspruntime.Event {
	z := zset.New()
	z.Insert(dbunstructured.New(doc, nil), w)
	return dbspruntime.Event{Name: name, Data: z}
}

func mustReceiveInput(ch <-chan dbspruntime.Event) dbspruntime.Event {
	var in dbspruntime.Event
	Eventually(ch, suite.Timeout, suite.Interval).Should(Receive(&in))
	return in
}

func weights(z zset.ZSet) []zset.Weight {
	w := make([]zset.Weight, 0, z.Size())
	for _, e := range z.Entries() {
		w = append(w, e.Weight)
	}
	sort.Slice(w, func(i, j int) bool { return w[i] < w[j] })
	return w
}

func singleField(z zset.ZSet, path ...string) string {
	values := allFieldValues(z, path...)
	Expect(values).To(HaveLen(1))
	return values[0]
}

func allFieldValues(z zset.ZSet, path ...string) []string {
	vals := make([]string, 0, z.Size())
	for _, e := range z.Entries() {
		doc, ok := e.Document.(*dbunstructured.Unstructured)
		Expect(ok).To(BeTrue())
		v, ok, err := unstructured.NestedString(doc.Fields(), path...)
		Expect(err).NotTo(HaveOccurred())
		if ok {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		Fail(fmt.Sprintf("field %v missing from all entries", path))
	}
	sort.Strings(vals)
	return vals
}
