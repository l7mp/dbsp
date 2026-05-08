package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	dbspjs "github.com/l7mp/dbsp/js"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func rawJSONSpec(s string) *apiextensionsv1.JSON {
	return &apiextensionsv1.JSON{Raw: []byte(s)}
}

func ptrString(v string) *string {
	return &v
}

func writeSuiteKubeconfig(suite *Suite) (string, error) {
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["envtest"] = &clientcmdapi.Cluster{
		Server:                   suite.Cfg.Host,
		InsecureSkipTLSVerify:    suite.Cfg.Insecure,
		CertificateAuthorityData: suite.Cfg.CAData,
	}
	cfg.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{
		Token:                 suite.Cfg.BearerToken,
		ClientCertificateData: suite.Cfg.CertData,
		ClientKeyData:         suite.Cfg.KeyData,
	}
	cfg.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "envtest"}
	cfg.CurrentContext = "envtest"

	out := filepath.Join(GinkgoT().TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*cfg, out); err != nil {
		return "", err
	}

	return out, nil
}

func setEnvScoped(key, value string) {
	old, had := os.LookupEnv(key)
	Expect(os.Setenv(key, value)).To(Succeed())
	DeferCleanup(func() {
		if had {
			Expect(os.Setenv(key, old)).To(Succeed())
			return
		}
		Expect(os.Unsetenv(key)).To(Succeed())
	})
}

func unsetEnvScoped(key string) {
	old, had := os.LookupEnv(key)
	Expect(os.Unsetenv(key)).To(Succeed())
	DeferCleanup(func() {
		if had {
			Expect(os.Setenv(key, old)).To(Succeed())
			return
		}
		Expect(os.Unsetenv(key)).To(Succeed())
	})
}

func startJSManagerVM(kubeconfigPath string) (*dbspjs.VM, <-chan error) {
	setEnvScoped("DCONTROLLER_API_SERVER_ENABLED", "false")
	setEnvScoped("DCONTROLLER_KUBECONFIG", kubeconfigPath)
	setEnvScoped("DCONTROLLER_LOG_LEVEL", "debug")
	unsetEnvScoped("DCONTROLLER_RUNTIME_CONFIG")

	scriptPath, err := filepath.Abs(filepath.Join("..", "dcontroller.js"))
	Expect(err).NotTo(HaveOccurred())

	vm, err := dbspjs.NewVM(logr.Discard())
	Expect(err).NotTo(HaveOccurred())
	Expect(vm.SetProcessArgv([]string{"dbsp", scriptPath})).To(Succeed())

	errCh := make(chan error, 1)
	go func() {
		errCh <- vm.RunFile(scriptPath)
	}()

	return vm, errCh
}

func makeServiceTypeOperator(name, annotationKey string) *opv1a1.Operator {
	pipeline := fmt.Sprintf(`[
		{"@select":{"@eq":["$.metadata.namespace","testnamespace"]}},
		{"@project":{
			"metadata":{
				"name":"$.metadata.name",
				"namespace":"$.metadata.namespace",
				"annotations":{"%s":"$.spec.type"}
			}
		}}
	]`, annotationKey)

	return &opv1a1.Operator{
		TypeMeta: metav1.TypeMeta{
			APIVersion: opv1a1.GroupVersion.String(),
			Kind:       "Operator",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: opv1a1.OperatorSpec{Controllers: []opv1a1.Controller{{
			Name: "svc-annotator",
			Sources: []opv1a1.Source{{
				Resource: opv1a1.Resource{Group: ptrString(""), Kind: "Service"},
			}},
			Pipeline: rawJSONSpec(pipeline),
			Targets: []opv1a1.Target{{
				Resource: opv1a1.Resource{Group: ptrString(""), Kind: "Service"},
				Type:     opv1a1.Patcher,
			}},
		}}},
	}
}

func makeInvalidOperator(name string) *opv1a1.Operator {
	return &opv1a1.Operator{
		TypeMeta: metav1.TypeMeta{APIVersion: opv1a1.GroupVersion.String(), Kind: "Operator"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: opv1a1.OperatorSpec{Controllers: []opv1a1.Controller{{
			Name:     "broken",
			Sources:  []opv1a1.Source{},
			Pipeline: rawJSONSpec(`[{"@project":{"$.":"$."}}]`),
			Targets:  []opv1a1.Target{{Resource: opv1a1.Resource{Kind: "SomeView"}}},
		}}},
	}
}

func makeRuntimeErrorOperator(name string) *opv1a1.Operator {
	return &opv1a1.Operator{
		TypeMeta: metav1.TypeMeta{APIVersion: opv1a1.GroupVersion.String(), Kind: "Operator"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: opv1a1.OperatorSpec{Controllers: []opv1a1.Controller{{
			Name: "runtime-fail",
			Sources: []opv1a1.Source{{
				Resource:  opv1a1.Resource{Group: ptrString(""), Kind: "Service"},
				Namespace: ptrString("testnamespace"),
			}},
			Pipeline: rawJSONSpec(`[
				{"@project":{
					"apiVersion":"v1",
					"kind":"ConfigMap",
					"metadata":{"name":"$.metadata.name"},
					"data":{"x":"1"}
				}}
			]`),
			Targets: []opv1a1.Target{{
				Resource: opv1a1.Resource{Group: ptrString(""), Kind: "ConfigMap"},
				Type:     opv1a1.Updater,
			}},
		}}},
	}
}

func operatorStatus(suite *Suite, name string) (opv1a1.OperatorStatus, error) {
	op := &opv1a1.Operator{}
	err := suite.K8sClient.Get(suite.Ctx, types.NamespacedName{Name: name}, op)
	if err != nil {
		return opv1a1.OperatorStatus{}, err
	}

	return op.Status, nil
}

var _ = Describe("JS dcontroller manager", Ordered, Label("js-manager"), func() {
	var (
		suite *Suite
		vm    *dbspjs.VM
		vmErr <-chan error
	)

	BeforeAll(func() {
		var err error
		suite, err = NewSuite(loglevel, filepath.Join("..", "config", "crd", "resources"))
		Expect(err).NotTo(HaveOccurred())

		kubeconfigPath, err := writeSuiteKubeconfig(suite)
		Expect(err).NotTo(HaveOccurred())

		vm, vmErr = startJSManagerVM(kubeconfigPath)
		Expect(vm).NotTo(BeNil())
	})

	AfterAll(func() {
		if vm != nil {
			vm.Close()
			Eventually(vmErr, 5*time.Second, 20*time.Millisecond).Should(Receive(BeNil()))
		}
		suite.Close()
	})

	It("reconciles operator create, modify, and delete lifecycle", func() {
		svc := TestSvc.DeepCopy()
		svc.SetName("js-lifecycle-service")
		svc.SetNamespace("testnamespace")
		svc.SetAnnotations(map[string]string{})
		Expect(suite.K8sClient.Create(suite.Ctx, svc)).To(Succeed())
		DeferCleanup(func() {
			_ = suite.K8sClient.Delete(suite.Ctx, svc)
		})

		op := makeServiceTypeOperator("js-lifecycle-operator", "dcontroller.io/service-type")
		Expect(suite.K8sClient.Create(suite.Ctx, op)).To(Succeed())
		DeferCleanup(func() {
			_ = suite.K8sClient.Delete(suite.Ctx, op)
		})

		Eventually(func() string {
			current := kobject.New()
			current.SetGroupVersionKind(svc.GroupVersionKind())
			err := suite.K8sClient.Get(suite.Ctx, types.NamespacedName{Name: svc.GetName(), Namespace: svc.GetNamespace()}, current)
			if err != nil {
				return ""
			}
			anns := current.GetAnnotations()
			return anns["dcontroller.io/service-type"]
		}, suite.Timeout, suite.Interval).Should(Equal("ClusterIP"))

		Eventually(func() metav1.ConditionStatus {
			status, err := operatorStatus(suite, op.GetName())
			if err != nil {
				return metav1.ConditionUnknown
			}
			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
			if cond == nil {
				return metav1.ConditionUnknown
			}
			return cond.Status
		}, suite.Timeout, suite.Interval).Should(Equal(metav1.ConditionTrue))

		_, err := ctrlutil.CreateOrUpdate(suite.Ctx, suite.K8sClient, op, func() error {
			op.Spec = makeServiceTypeOperator("js-lifecycle-operator", "dcontroller.io/service-type-v2").Spec
			return nil
		})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() error {
			_, err = ctrlutil.CreateOrUpdate(suite.Ctx, suite.K8sClient, svc, func() error {
				return unstructured.SetNestedField(svc.Object, "NodePort", "spec", "type")
			})
			return err
		}, suite.Timeout, suite.Interval).Should(Succeed())

		Eventually(func() string {
			current := kobject.New()
			current.SetGroupVersionKind(svc.GroupVersionKind())
			err := suite.K8sClient.Get(suite.Ctx, types.NamespacedName{Name: svc.GetName(), Namespace: svc.GetNamespace()}, current)
			if err != nil {
				return ""
			}
			anns := current.GetAnnotations()
			return anns["dcontroller.io/service-type-v2"]
		}, suite.Timeout, suite.Interval).Should(Equal("NodePort"))

		Expect(suite.K8sClient.Delete(suite.Ctx, op)).To(Succeed())
		Eventually(func() bool {
			get := &opv1a1.Operator{}
			err := suite.K8sClient.Get(suite.Ctx, types.NamespacedName{Name: op.GetName()}, get)
			return apierrors.IsNotFound(err)
		}, suite.Timeout, suite.Interval).Should(BeTrue())

		Eventually(func() error {
			_, err = ctrlutil.CreateOrUpdate(suite.Ctx, suite.K8sClient, svc, func() error {
				return unstructured.SetNestedField(svc.Object, "LoadBalancer", "spec", "type")
			})
			return err
		}, suite.Timeout, suite.Interval).Should(Succeed())

		Consistently(func() string {
			current := kobject.New()
			current.SetGroupVersionKind(svc.GroupVersionKind())
			err := suite.K8sClient.Get(suite.Ctx, types.NamespacedName{Name: svc.GetName(), Namespace: svc.GetNamespace()}, current)
			if err != nil {
				return ""
			}
			anns := current.GetAnnotations()
			return anns["dcontroller.io/service-type-v2"]
		}, 1200*time.Millisecond, 100*time.Millisecond).Should(Equal("NodePort"))
	})

	It("publishes NotReady status for invalid operator specs", func() {
		op := makeInvalidOperator("js-invalid-operator")
		Expect(suite.K8sClient.Create(suite.Ctx, op)).To(Succeed())
		DeferCleanup(func() {
			_ = suite.K8sClient.Delete(suite.Ctx, op)
		})

		Eventually(func() string {
			status, err := operatorStatus(suite, op.GetName())
			if err != nil {
				return ""
			}
			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
			if cond == nil {
				return ""
			}
			if cond.Status != metav1.ConditionFalse {
				return ""
			}
			return cond.Reason
		}, suite.Timeout, suite.Interval).Should(Equal(string(opv1a1.OperatorReasonNotReady)))

		Eventually(func() int {
			status, err := operatorStatus(suite, op.GetName())
			if err != nil {
				return 0
			}
			return len(status.LastErrors)
		}, suite.Timeout, suite.Interval).Should(BeNumerically(">", 0))
	})

	It("caps runtime errors to the last five messages", func() {
		op := makeRuntimeErrorOperator("js-runtime-error-operator")
		Expect(suite.K8sClient.Create(suite.Ctx, op)).To(Succeed())
		DeferCleanup(func() {
			_ = suite.K8sClient.Delete(suite.Ctx, op)
		})

		for i := 0; i < 8; i += 1 {
			svc := TestSvc.DeepCopy()
			svc.SetName(fmt.Sprintf("js-runtime-error-service-%s", strconv.Itoa(i)))
			svc.SetNamespace("testnamespace")
			Expect(suite.K8sClient.Create(suite.Ctx, svc)).To(Succeed())
			DeferCleanup(func() {
				_ = suite.K8sClient.Delete(suite.Ctx, svc)
			})
		}

		Eventually(func() metav1.ConditionStatus {
			status, err := operatorStatus(suite, op.GetName())
			if err != nil {
				return metav1.ConditionUnknown
			}
			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
			if cond == nil {
				return metav1.ConditionUnknown
			}
			return cond.Status
		}, 6*time.Second, 100*time.Millisecond).Should(Equal(metav1.ConditionFalse))

		Eventually(func() int {
			status, err := operatorStatus(suite, op.GetName())
			if err != nil {
				return 0
			}
			return len(status.LastErrors)
		}, 6*time.Second, 100*time.Millisecond).Should(Equal(5))

		Eventually(func() string {
			status, err := operatorStatus(suite, op.GetName())
			if err != nil {
				return ""
			}
			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
			if cond == nil {
				return ""
			}
			return cond.Reason
		}, suite.Timeout, suite.Interval).Should(Equal(string(opv1a1.OperatorReasonReconciliationFailed)))
	})
})
