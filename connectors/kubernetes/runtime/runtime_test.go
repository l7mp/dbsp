package runtime_test

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	kruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	kauth "github.com/l7mp/dbsp/connectors/kubernetes/runtime/auth"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	rtstore "github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	runtimeTestTimeout  = 2 * time.Second
	runtimeTestInterval = 20 * time.Millisecond
)

func receiveWatchEvent(w watch.Interface, d time.Duration) (watch.Event, bool) {
	select {
	case e, ok := <-w.ResultChan():
		if !ok {
			return watch.Event{}, false
		}
		return e, true
	case <-time.After(d):
		return watch.Event{}, false
	}
}

var _ = Describe("Runtime", func() {
	It("creates a headless runtime and wires view cache, discovery, client, watch, and API server", func() {
		apicfg, err := kruntime.NewDefaultAPIServerConfig("127.0.0.1", 0, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		apicfg.EnableOpenAPI = false

		rt, err := kruntime.New(kruntime.Config{
			RESTConfig: nil,
			CacheOptions: rtstore.CacheOptions{
				DefaultCache: rtstore.NewFakeRuntimeCache(scheme.Scheme),
			},
			Auth:      &kruntime.AuthConfig{},
			APIServer: &apicfg,
			Logger:    logr.Discard(),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(rt).NotTo(BeNil())

		Expect(rt.GetClient()).NotTo(BeNil())
		Expect(rt.GetCache()).NotTo(BeNil())
		Expect(rt.GetViewCache()).NotTo(BeNil())
		Expect(rt.GetDiscovery()).NotTo(BeNil())
		Expect(rt.GetRESTMapper()).NotTo(BeNil())
		Expect(rt.GetAPIServer()).NotTo(BeNil())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()

		gvk := viewv1a1.GroupVersionKind("testruntime", "Widget")
		Expect(rt.GetAPIServer().RegisterGVKs([]schema.GroupVersionKind{gvk})).To(Succeed())
		Expect(rt.GetDiscovery().RegisterViewGVK(gvk)).To(Succeed())

		obj := object.NewViewObject("testruntime", "Widget")
		object.SetName(obj, "default", "widget-1")
		object.SetContent(obj, map[string]any{"spec": map[string]any{"value": "v1"}})

		Expect(rt.GetViewCache().Add(obj)).To(Succeed())

		By("discovering the registered view group and resources")
		Eventually(func() bool {
			groups, err := rt.GetDiscovery().ServerGroups()
			if err != nil {
				return false
			}
			for _, g := range groups.Groups {
				if g.Name == viewv1a1.Group("testruntime") {
					return true
				}
			}
			return false
		}, runtimeTestTimeout, runtimeTestInterval).Should(BeTrue())

		resources, err := rt.GetDiscovery().ServerResourcesForGroupVersion(viewv1a1.GroupVersion("testruntime").String())
		Expect(err).NotTo(HaveOccurred())
		Expect(resources.APIResources).NotTo(BeEmpty())

		By("querying the object through composite client")
		fetched := object.NewViewObject("testruntime", "Widget")
		object.SetName(fetched, "default", "widget-1")
		Eventually(func() error {
			return rt.GetClient().Get(context.Background(), client.ObjectKeyFromObject(fetched), fetched)
		}, runtimeTestTimeout, runtimeTestInterval).Should(Succeed())

		value, found, err := unstructured.NestedString(fetched.Object, "spec", "value")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(value).To(Equal("v1"))

		By("listing through composite client")
		list := rtstore.NewViewObjectList("testruntime", "Widget")
		Eventually(func() int {
			err := rt.GetClient().List(context.Background(), list)
			if err != nil {
				return -1
			}
			return len(list.Items)
		}, runtimeTestTimeout, runtimeTestInterval).Should(Equal(1))

		By("watching initial and update events through composite client")
		watchList := rtstore.NewViewObjectList("testruntime", "Widget")
		w, err := rt.GetClient().Watch(context.Background(), watchList)
		Expect(err).NotTo(HaveOccurred())
		defer w.Stop()

		initial, ok := receiveWatchEvent(w, runtimeTestTimeout)
		Expect(ok).To(BeTrue())
		Expect(initial.Type).To(Equal(watch.Added))
		added, ok := initial.Object.(object.Object)
		Expect(ok).To(BeTrue())
		Expect(added.GetName()).To(Equal("widget-1"))

		updated := object.DeepCopy(obj)
		object.SetContent(updated, map[string]any{"spec": map[string]any{"value": "v2"}})
		Expect(rt.GetViewCache().Update(obj, updated)).To(Succeed())

		event, ok := receiveWatchEvent(w, runtimeTestTimeout)
		Expect(ok).To(BeTrue())
		Expect(event.Type).To(Equal(watch.Modified))
		mod, ok := event.Object.(object.Object)
		Expect(ok).To(BeTrue())
		mv, found, err := unstructured.NestedString(mod.Object, "spec", "value")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(mv).To(Equal("v2"))

		By("querying discovery through kubeconfig-generated credentials")
		addr := rt.GetAPIServer().GetInsecureServerAddress()
		Expect(addr).NotTo(BeEmpty())
		Expect(addr).NotTo(Equal("<unknown>"))

		kopts := kauth.DefaultKubeconfigOptions()
		kopts.HTTPMode = true
		kopts.Insecure = true
		kcfg, err := rt.GenerateKubeconfig("alice", kopts)
		Expect(err).NotTo(HaveOccurred())
		rawCfg, err := restConfigFromGeneratedKubeconfig(kcfg)
		Expect(err).NotTo(HaveOccurred())

		dc, err := discovery.NewDiscoveryClientForConfig(rawCfg)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() error {
			_, err := dc.ServerVersion()
			return err
		}, runtimeTestTimeout, runtimeTestInterval).Should(Succeed())

		By("querying the view object through kubeconfig-generated credentials")
		dyn, err := dynamic.NewForConfig(rawCfg)
		Expect(err).NotTo(HaveOccurred())
		gvr := schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: "widget"}

		var got *unstructured.Unstructured
		Eventually(func() error {
			obj, e := dyn.Resource(gvr).Namespace("default").Get(context.Background(), "widget-1", metav1.GetOptions{})
			if e != nil {
				return e
			}
			got = obj
			return nil
		}, runtimeTestTimeout, runtimeTestInterval).Should(Succeed())
		vv, found, err := unstructured.NestedString(got.Object, "spec", "value")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(vv).To(Equal("v2"))

		cancel()
		Eventually(errCh, runtimeTestTimeout).Should(Receive(BeNil()))
	})

	It("stays headless for native resources when RESTConfig is nil", func() {
		rt, err := kruntime.New(kruntime.Config{
			RESTConfig: nil,
			CacheOptions: rtstore.CacheOptions{
				DefaultCache: rtstore.NewFakeRuntimeCache(scheme.Scheme),
			},
			Logger: logr.Discard(),
		})
		Expect(err).NotTo(HaveOccurred())

		pod := object.New()
		pod.SetAPIVersion("v1")
		pod.SetKind("Pod")
		pod.SetName("pod-1")
		pod.SetNamespace("default")

		err = rt.GetClient().Get(context.Background(), client.ObjectKeyFromObject(pod), pod)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsNotFound(err) || apierrors.IsBadRequest(err)).To(BeTrue())

		_, err = rt.GetDiscovery().ServerResourcesForGroupVersion("v1")
		Expect(err).To(HaveOccurred())
	})

	It("returns errors for auth helpers when auth is disabled", func() {
		apicfg, err := kruntime.NewDefaultAPIServerConfig("127.0.0.1", 0, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		apicfg.EnableOpenAPI = false

		rt, err := kruntime.New(kruntime.Config{RESTConfig: nil, APIServer: &apicfg, Logger: logr.Discard()})
		Expect(err).NotTo(HaveOccurred())

		_, err = rt.GenerateKubeconfig("alice", nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no auth configured"))

		_, err = rt.GetClientConfig("alice")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no auth configured"))
	})

	It("returns errors for auth helpers when API server is disabled", func() {
		rt, err := kruntime.New(kruntime.Config{RESTConfig: nil, Logger: logr.Discard()})
		Expect(err).NotTo(HaveOccurred())

		_, err = rt.GenerateKubeconfig("alice", nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no API server configured"))

		_, err = rt.GetClientConfig("alice")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no API server configured"))
	})

	It("maps views and rejects native resources through the composite REST mapper in headless mode", func() {
		rt, err := kruntime.New(kruntime.Config{RESTConfig: nil, Logger: logr.Discard()})
		Expect(err).NotTo(HaveOccurred())

		gvk := viewv1a1.GroupVersionKind("testruntime", "Widget")
		Expect(rt.GetDiscovery().RegisterViewGVK(gvk)).To(Succeed())

		mapping, err := rt.GetRESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
		Expect(err).NotTo(HaveOccurred())
		Expect(mapping.Resource.Group).To(Equal(gvk.Group))
		Expect(mapping.Resource.Version).To(Equal(gvk.Version))
		Expect(mapping.Resource.Resource).To(Equal("widget"))

		kind, err := rt.GetRESTMapper().KindFor(schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: "widget"})
		Expect(err).NotTo(HaveOccurred())
		Expect(kind).To(Equal(gvk))

		_, err = rt.GetRESTMapper().RESTMapping(schema.GroupKind{Group: "apps", Kind: "Deployment"}, "v1")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(SatisfyAny(
			ContainSubstring("no RESTMapper available"),
			ContainSubstring("no matches for kind"),
		))

		_, err = rt.GetRESTMapper().KindFor(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(SatisfyAny(
			ContainSubstring("no RESTMapper available"),
			ContainSubstring("no matches for"),
		))
	})

	It("generates and validates auth-enabled client material", func() {
		apicfg, err := kruntime.NewDefaultAPIServerConfig("127.0.0.1", 0, true, false, logr.Discard())
		Expect(err).NotTo(HaveOccurred())
		apicfg.EnableOpenAPI = false

		rt, err := kruntime.New(kruntime.Config{
			RESTConfig: nil,
			CacheOptions: rtstore.CacheOptions{
				DefaultCache: rtstore.NewFakeRuntimeCache(scheme.Scheme),
			},
			APIServer: &apicfg,
			Auth:      &kruntime.AuthConfig{},
			Logger:    logr.Discard(),
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()

		kopts := kauth.DefaultKubeconfigOptions()
		kopts.HTTPMode = true
		kopts.Insecure = true
		cfg, err := rt.GenerateKubeconfig("alice", kopts)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg).NotTo(BeNil())

		restCfg, err := restConfigFromGeneratedKubeconfig(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(restCfg).NotTo(BeNil())
		Expect(restCfg.BearerToken).NotTo(BeEmpty())

		dyn, err := dynamic.NewForConfig(restCfg)
		Expect(err).NotTo(HaveOccurred())
		dc, err := discovery.NewDiscoveryClientForConfig(restCfg)
		Expect(err).NotTo(HaveOccurred())

		gvk := viewv1a1.GroupVersionKind("testruntimeauth", "AuthWidget")
		Expect(rt.GetAPIServer().RegisterGVKs([]schema.GroupVersionKind{gvk})).To(Succeed())
		Expect(rt.GetDiscovery().RegisterViewGVK(gvk)).To(Succeed())

		obj := object.NewViewObject("testruntimeauth", "AuthWidget")
		object.SetName(obj, "default", "auth-widget-1")
		object.SetContent(obj, map[string]any{"spec": map[string]any{"value": "ok"}})
		Expect(rt.GetViewCache().Add(obj)).To(Succeed())

		gvr := schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: "authwidget"}
		Eventually(func() error {
			_, e := dyn.Resource(gvr).Namespace("default").Get(context.Background(), "auth-widget-1", metav1.GetOptions{})
			return e
		}, runtimeTestTimeout, runtimeTestInterval).Should(Succeed())

		Eventually(func() bool {
			groups, err := dc.ServerGroups()
			if err != nil {
				return false
			}
			for _, g := range groups.Groups {
				if g.Name == viewv1a1.Group("testruntimeauth") {
					return true
				}
			}
			return false
		}, runtimeTestTimeout, runtimeTestInterval).Should(BeTrue())

		cancel()
		Eventually(errCh, runtimeTestTimeout).Should(Receive(BeNil()))
	})
})

func restConfigFromGeneratedKubeconfig(cfg *clientcmdapi.Config) (*rest.Config, error) {
	if cfg == nil {
		return nil, apierrors.NewBadRequest("kubeconfig is nil")
	}
	ctx, ok := cfg.Contexts[cfg.CurrentContext]
	if !ok {
		return nil, apierrors.NewBadRequest("current context not found")
	}
	cluster, ok := cfg.Clusters[ctx.Cluster]
	if !ok {
		return nil, apierrors.NewBadRequest("cluster not found")
	}
	auth, ok := cfg.AuthInfos[ctx.AuthInfo]
	if !ok {
		return nil, apierrors.NewBadRequest("auth info not found")
	}

	return &rest.Config{
		Host:        cluster.Server,
		BearerToken: auth.Token,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: cluster.InsecureSkipTLSVerify,
		},
	}, nil
}
