package operator

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	kauth "github.com/l7mp/dbsp/connectors/kubernetes/runtime/auth"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	operatorTestTimeout  = 3 * time.Second
	operatorTestInterval = 20 * time.Millisecond
)

func rawJSON(s string) *apiextensionsv1.JSON {
	return &apiextensionsv1.JSON{Raw: []byte(s)}
}

func makeHeadlessRuntime(name string) *k8sruntime.Runtime {
	apicfg, err := k8sruntime.NewDefaultAPIServerConfig("127.0.0.1", 0, true, false, logr.Discard())
	Expect(err).NotTo(HaveOccurred())
	apicfg.EnableOpenAPI = false

	rt, err := k8sruntime.New(k8sruntime.Config{
		RESTConfig: nil,
		CacheOptions: store.CacheOptions{
			DefaultCache: store.NewFakeRuntimeCache(apiruntime.NewScheme()),
		},
		APIServer: &apicfg,
		Auth:      &k8sruntime.AuthConfig{},
		Logger:    logr.Discard().WithName("k8s-runtime").WithValues("name", name),
	})
	Expect(err).NotTo(HaveOccurred())
	return rt
}

func makeSimpleControllerSpec(name, srcKind, targetKind string) opv1a1.Controller {
	return opv1a1.Controller{
		Name: name,
		Sources: []opv1a1.Source{{
			Resource: opv1a1.Resource{Kind: srcKind},
			Type:     opv1a1.Watcher,
		}},
		Pipeline: rawJSON(`[
			{"@project":{
				"metadata":{"name":"$.metadata.name","namespace":"$.metadata.namespace"},
				"spec":{"value":"$.spec.value","copied":true}
			}}
		]`),
		Targets: []opv1a1.Target{{Resource: opv1a1.Resource{Kind: targetKind}, Type: opv1a1.Updater}},
	}
}

func makeOperatorCR(name string, ctrl opv1a1.Controller) *opv1a1.Operator {
	return &opv1a1.Operator{
		TypeMeta: metav1.TypeMeta{APIVersion: opv1a1.GroupVersion.String(), Kind: "Operator"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: opv1a1.OperatorSpec{Controllers: []opv1a1.Controller{ctrl}},
	}
}

func toViewOperatorObject(group string, op *opv1a1.Operator) object.Object {
	u, err := apiruntime.DefaultUnstructuredConverter.ToUnstructured(op)
	Expect(err).NotTo(HaveOccurred())

	v := object.NewViewObject(group, "Operator")
	object.SetContent(v, u)
	object.SetName(v, "", op.GetName())
	v.SetGroupVersionKind(operatorGVK)
	return v
}

func restConfigFromGeneratedKubeconfig(cfg *clientcmdapi.Config) (*rest.Config, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kubeconfig is nil")
	}
	ctx, ok := cfg.Contexts[cfg.CurrentContext]
	if !ok {
		return nil, fmt.Errorf("current context not found")
	}
	cluster, ok := cfg.Clusters[ctx.Cluster]
	if !ok {
		return nil, fmt.Errorf("cluster not found")
	}
	auth, ok := cfg.AuthInfos[ctx.AuthInfo]
	if !ok {
		return nil, fmt.Errorf("auth info not found")
	}

	return &rest.Config{
		Host:        cluster.Server,
		BearerToken: auth.Token,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: cluster.InsecureSkipTLSVerify,
		},
	}, nil
}

var _ = Describe("Operator", func() {
	It("runs a direct operator over headless view runtime and transforms view objects", func() {
		krt := makeHeadlessRuntime("direct")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- krt.Start(ctx) }()

		spec := opv1a1.OperatorSpec{Controllers: []opv1a1.Controller{
			makeSimpleControllerSpec("copy-controller", "Foo", "Bar"),
		}}
		op, err := New("test-op", Config{Spec: spec, K8sRuntime: krt, Logger: logr.Discard()})
		Expect(err).NotTo(HaveOccurred())

		go func() { _ = op.Start(ctx) }()

		src := object.NewViewObject("test-op", "Foo")
		object.SetName(src, "default", "foo-1")
		object.SetContent(src, map[string]any{
			"metadata": map[string]any{"name": "foo-1", "namespace": "default"},
			"spec":     map[string]any{"value": "alpha"},
		})
		Expect(krt.GetViewCache().Add(src)).To(Succeed())

		out := object.NewViewObject("test-op", "Bar")
		object.SetName(out, "default", "foo-1")
		Eventually(func() error {
			return krt.GetClient().Get(context.Background(), client.ObjectKeyFromObject(out), out)
		}, operatorTestTimeout, operatorTestInterval).Should(Succeed())

		val, ok, err := nestedString(out.Object, "spec", "value")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("alpha"))

		copied, ok, err := nestedBool(out.Object, "spec", "copied")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(copied).To(BeTrue())

		resources, err := krt.GetDiscovery().ServerResourcesForGroupVersion(viewv1a1.GroupVersion("test-op").String())
		Expect(err).NotTo(HaveOccurred())
		Expect(resources.APIResources).To(BeEmpty())

		kopts := kauth.DefaultKubeconfigOptions()
		kopts.HTTPMode = true
		kopts.Insecure = true
		kcfg, err := krt.GenerateKubeconfig("alice", kopts)
		Expect(err).NotTo(HaveOccurred())
		restCfg, err := restConfigFromGeneratedKubeconfig(kcfg)
		Expect(err).NotTo(HaveOccurred())

		dcAPI, err := discovery.NewDiscoveryClientForConfig(restCfg)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() bool {
			groups, err := dcAPI.ServerGroups()
			if err != nil {
				return false
			}
			for _, g := range groups.Groups {
				if g.Name == viewv1a1.Group("test-op") {
					return true
				}
			}
			return false
		}, operatorTestTimeout, operatorTestInterval).Should(BeTrue())

		dyn, err := dynamic.NewForConfig(restCfg)
		Expect(err).NotTo(HaveOccurred())
		gvr := schema.GroupVersionResource{Group: viewv1a1.Group("test-op"), Version: viewv1a1.Version, Resource: "bar"}
		Eventually(func() error {
			_, e := dyn.Resource(gvr).Namespace("default").Get(context.Background(), "foo-1", metav1.GetOptions{})
			return e
		}, operatorTestTimeout, operatorTestInterval).Should(Succeed())

		cancel()
		Eventually(errCh, operatorTestTimeout).Should(Receive(BeNil()))
	})

	It("reconciles an Operator CR via OperatorController in headless mode", func() {
		old := operatorGVK
		operatorGVK = viewv1a1.GroupVersionKind("dcontroller", "Operator")
		DeferCleanup(func() { operatorGVK = old })

		cfg := k8sruntime.Config{Logger: logr.Discard(), RESTConfig: nil}
		oc, err := NewOperatorController(cfg)
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- oc.Start(ctx) }()

		ctrl := makeSimpleControllerSpec("oc-copy", "Foo", "Bar")
		opCR := makeOperatorCR("oc-op", ctrl)

		viewOp := toViewOperatorObject("dcontroller", opCR)
		Expect(oc.k8sRuntime.GetViewCache().Add(viewOp)).To(Succeed())

		src := object.NewViewObject("oc-op", "Foo")
		object.SetName(src, "default", "foo-oc")
		object.SetContent(src, map[string]any{
			"metadata": map[string]any{"name": "foo-oc", "namespace": "default"},
			"spec":     map[string]any{"value": "beta"},
		})
		Expect(oc.k8sRuntime.GetViewCache().Add(src)).To(Succeed())

		out := object.NewViewObject("oc-op", "Bar")
		object.SetName(out, "default", "foo-oc")
		Eventually(func() error {
			return oc.GetClient().Get(context.Background(), client.ObjectKeyFromObject(out), out)
		}, operatorTestTimeout, operatorTestInterval).Should(Succeed())

		val, ok, err := nestedString(out.Object, "spec", "value")
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(val).To(Equal("beta"))

		statusObj := object.NewViewObject("dcontroller", "Operator")
		object.SetName(statusObj, "", "oc-op")
		Eventually(func() error {
			return oc.GetClient().Get(context.Background(), client.ObjectKey{Name: "oc-op"}, statusObj)
		}, operatorTestTimeout, operatorTestInterval).Should(Succeed())

		cancel()
		Eventually(errCh, operatorTestTimeout).Should(Receive(BeNil()))
	})

	It("surfaces fail-pipeline errors through runtime error reporting", func() {
		old := operatorGVK
		operatorGVK = viewv1a1.GroupVersionKind("dcontroller", "Operator")
		DeferCleanup(func() { operatorGVK = old })

		oc, err := NewOperatorController(k8sruntime.Config{Logger: logr.Discard(), RESTConfig: nil})
		Expect(err).NotTo(HaveOccurred())

		reportCh := make(chan dbspruntime.Error, 16)
		oc.dbspRuntime.SetErrorChannel(reportCh)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = oc.Start(ctx) }()

		bad := opv1a1.Controller{
			Name: "fail-pipeline",
			Sources: []opv1a1.Source{{
				Resource:   opv1a1.Resource{Kind: "Foo"},
				Type:       opv1a1.Periodic,
				Parameters: rawJSON(`{"period":"50ms"}`),
			}},
			Pipeline: rawJSON(`[{"@unknown":true}]`),
			Targets:  []opv1a1.Target{{Resource: opv1a1.Resource{Kind: "Bar"}}},
		}

		opCR := makeOperatorCR("err-op", bad)
		viewOp := toViewOperatorObject("dcontroller", opCR)
		Expect(oc.k8sRuntime.GetViewCache().Add(viewOp)).To(Succeed())

		var runtimeErr dbspruntime.Error
		Eventually(reportCh, operatorTestTimeout, operatorTestInterval).Should(Receive(&runtimeErr))
		Expect(runtimeErr.Origin).To(Equal(processorComponentName))
		Expect(runtimeErr.Err).NotTo(BeNil())
		Expect(runtimeErr.Err.Error()).To(ContainSubstring("failed to create controller"))
		Expect(runtimeErr.Err.Error()).To(ContainSubstring("fail-pipeline"))
	})
})

func nestedString(m map[string]any, fields ...string) (string, bool, error) {
	if len(fields) == 0 {
		return "", false, fmt.Errorf("fields are required")
	}
	cur := m
	for _, field := range fields[:len(fields)-1] {
		next, ok := cur[field].(map[string]any)
		if !ok {
			return "", false, nil
		}
		cur = next
	}
	v, ok := cur[fields[len(fields)-1]]
	if !ok {
		return "", false, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", false, fmt.Errorf("field %q is not a string", fields[len(fields)-1])
	}
	return s, true, nil
}

func nestedBool(m map[string]any, fields ...string) (bool, bool, error) {
	if len(fields) == 0 {
		return false, false, fmt.Errorf("fields are required")
	}
	cur := m
	for _, field := range fields[:len(fields)-1] {
		next, ok := cur[field].(map[string]any)
		if !ok {
			return false, false, nil
		}
		cur = next
	}
	v, ok := cur[fields[len(fields)-1]]
	if !ok {
		return false, false, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, false, fmt.Errorf("field %q is not a bool", fields[len(fields)-1])
	}
	return b, true, nil
}
