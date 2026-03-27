package producer

import (
	"context"
	"sort"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

var _ = Describe("Kubernetes lister producer", func() {
	It("emits full snapshots on watch events", func() {
		scheme := kruntime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

		first := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm-a", Namespace: "default", Labels: map[string]string{"app": "x"}},
			Data:       map[string]string{"k": "1"},
		}
		second := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm-b", Namespace: "other", Labels: map[string]string{"app": "x"}},
			Data:       map[string]string{"k": "2"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, second).Build()
		wc := &fakeWithWatchClient{Client: c, watch: watch.NewFake()}

		rt := dbspruntime.NewRuntime(logr.Discard())
		sub := rt.NewSubscriber()
		sub.Subscribe("in")

		p, err := NewLister(Config{
			Name:      "lister-test",
			Client:    wc,
			SourceGVK: schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			InputName: "in",
			Namespace: "default",
			Runtime:   rt,
			Logger:    logr.Discard(),
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- p.Start(ctx) }()

		wc.watch.Add(triggerEventObject("cm-a", "default"))

		var evt dbspruntime.Event
		Eventually(sub.GetChannel(), "2s", "10ms").Should(Receive(&evt))
		Expect(evt.Name).To(Equal("in"))
		Expect(evt.Data.Size()).To(Equal(1))
		Expect(singleField(evt.Data, "metadata", "name")).To(Equal("cm-a"))

		third := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm-c", Namespace: "default", Labels: map[string]string{"app": "x"}},
			Data:       map[string]string{"k": "3"},
		}
		Expect(c.Create(context.Background(), third)).To(Succeed())
		wc.watch.Modify(triggerEventObject("cm-c", "default"))

		Eventually(sub.GetChannel(), "2s", "10ms").Should(Receive(&evt))
		Expect(evt.Data.Size()).To(Equal(2))
		Expect(allNames(evt.Data)).To(Equal([]string{"cm-a", "cm-c"}))

		cancel()
		Eventually(errCh, "2s", "10ms").Should(Receive(BeNil()))
	})

	It("applies predicate filtering after listing", func() {
		scheme := kruntime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

		first := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm-a", Namespace: "default", Labels: map[string]string{"keep": "yes"}},
			Data:       map[string]string{"k": "1"},
		}
		second := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm-b", Namespace: "default", Labels: map[string]string{"keep": "no"}},
			Data:       map[string]string{"k": "2"},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(first, second).Build()
		wc := &fakeWithWatchClient{Client: c, watch: watch.NewFake()}

		rt := dbspruntime.NewRuntime(logr.Discard())
		sub := rt.NewSubscriber()
		sub.Subscribe("in")

		sel := &metav1.LabelSelector{MatchLabels: map[string]string{"keep": "yes"}}
		p, err := NewLister(Config{
			Name:          "lister-label-test",
			Client:        wc,
			SourceGVK:     schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
			InputName:     "in",
			LabelSelector: sel,
			Runtime:       rt,
			Logger:        logr.Discard(),
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- p.Start(ctx) }()

		wc.watch.Add(triggerEventObject("cm-a", "default"))

		var evt dbspruntime.Event
		Eventually(sub.GetChannel(), "2s", "10ms").Should(Receive(&evt))
		Expect(evt.Data.Size()).To(Equal(1))
		Expect(singleField(evt.Data, "metadata", "name")).To(Equal("cm-a"))

		cancel()
		Eventually(errCh, "2s", "10ms").Should(Receive(BeNil()))
	})
})

type fakeWithWatchClient struct {
	client.Client
	watch *watch.FakeWatcher
}

func (f *fakeWithWatchClient) Watch(_ context.Context, _ client.ObjectList, _ ...client.ListOption) (watch.Interface, error) {
	if f.watch == nil {
		return nil, context.Canceled
	}
	return f.watch, nil
}

func allNames(z zset.ZSet) []string {
	vals := allFieldValues(z, "metadata", "name")
	sort.Strings(vals)
	return vals
}

func singleField(z zset.ZSet, path ...string) string {
	vals := allFieldValues(z, path...)
	Expect(vals).To(HaveLen(1))
	return vals[0]
}

func allFieldValues(z zset.ZSet, path ...string) []string {
	vals := []string{}
	for _, e := range z.Entries() {
		u, ok := e.Document.(*dbunstructured.Unstructured)
		Expect(ok).To(BeTrue())
		v, ok, err := unstructured.NestedString(u.Fields(), path...)
		Expect(err).NotTo(HaveOccurred())
		if ok {
			vals = append(vals, v)
		}
	}
	return vals
}

func triggerEventObject(name, namespace string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace(namespace)
	obj.SetName(name)
	return obj
}
