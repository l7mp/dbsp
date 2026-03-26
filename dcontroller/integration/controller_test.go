package integration

import (
	"context"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	"github.com/l7mp/dbsp/dcontroller/controller"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

var _ = Describe("Controller test:", Ordered, func() {
	// write service type into an annotation for services running in the default namespace
	Context("When applying a self-referencial controller", Ordered, Label("controller"), func() {
		const annotationName = "service-type"
		var (
			suite      *Suite
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
			svc        object.Object
			gvk        schema.GroupVersionKind
			runtime    *dbspruntime.Runtime
			k8sRuntime *k8sruntime.Runtime
		)

		BeforeAll(func() {
			var err error
			suite, err = NewSuite(loglevel, filepath.Join("..", "config", "crd", "resources"))
			Expect(err).NotTo(HaveOccurred())

			ctrlCtx, ctrlCancel = context.WithCancel(suite.Ctx)
			svc = TestSvc.DeepCopy()
			gvk = schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Service",
			}
			svc.SetGroupVersionKind(gvk)
		})

		AfterAll(func() {
			ctrlCancel()
			suite.Close()
		})

		It("should create and start runtimes successfully", func() {
			suite.Log.Info("setting up runtimes")
			k8srt, err := k8sruntime.New(k8sruntime.Config{RESTConfig: suite.Cfg, Logger: suite.Log})
			Expect(err).NotTo(HaveOccurred())
			k8sRuntime = k8srt

			runtime = dbspruntime.NewRuntime(suite.Log)

			suite.Log.Info("starting kubernetes runtime")
			go func() {
				defer GinkgoRecover()
				err := k8sRuntime.Start(ctrlCtx)
				Expect(err).NotTo(HaveOccurred(), "failed to run kubernetes runtime")
			}()

			suite.Log.Info("starting dbsp runtime")
			go func() {
				defer GinkgoRecover()
				err := runtime.Start(ctrlCtx)
				Expect(err).NotTo(HaveOccurred(), "failed to run dbsp runtime")
			}()
		})

		It("should let a controller to be attached to the manager", func() {
			yamlData := `
name: svc-annotator
sources:
  - apiGroup: ""
    kind: Service
pipeline:
  - "@project":
      metadata:
        name: "$.metadata.name"
        namespace: "$.metadata.namespace"
        annotations:
          "service-type": "$.spec.type"
targets:
  - apiGroup: ""
    kind: Service
    type: Patcher
`

			var config opv1a1.Controller
			Expect(yaml.Unmarshal([]byte(yamlData), &config)).NotTo(HaveOccurred())

			c, err := controller.New(controller.Config{
				OperatorName: "test-controller",
				Spec:         config,
				Runtime:      runtime,
				K8sRuntime:   k8sRuntime,
				Logger:       suite.Log,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetName()).To(Equal("svc-annotator"))
		})

		It("should add the clusterIP annotation to a service", func() {
			suite.Log.Info("loading service")
			Expect(suite.K8sClient.Create(ctrlCtx, svc)).Should(Succeed())

			get := object.New()
			get.SetGroupVersionKind(gvk)
			key := client.ObjectKeyFromObject(svc)
			Eventually(func() bool {
				// if err := mgr.GetClient().Get(suite.Ctx, key, get); err != nil && apierrors.IsNotFound(err) {
				if err := suite.K8sClient.Get(suite.Ctx, key, get); err != nil && apierrors.IsNotFound(err) {
					suite.Log.Info("could not query starting manager")
					return false
				}

				if get.GetName() != key.Name || get.GetNamespace() != key.Namespace {
					return false
				}

				serviceType, ok, err := unstructured.NestedString(get.Object, "spec", "type")
				if err != nil || !ok {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[annotationName] == serviceType
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should adjust the annotation when the service type is manually updated", func() {
			suite.Log.Info("updating service service")

			_, err := ctrlutil.CreateOrUpdate(ctrlCtx, suite.K8sClient, svc, func() error {
				return unstructured.SetNestedField(svc.UnstructuredContent(), "NodePort", "spec", "type")
			})
			Expect(err).Should(Succeed())

			get := object.New()
			get.SetGroupVersionKind(gvk)
			key := client.ObjectKeyFromObject(svc)
			Eventually(func() bool {
				if err := suite.K8sClient.Get(suite.Ctx, key, get); err != nil && apierrors.IsNotFound(err) {
					suite.Log.Info("could not query starting manager")
					return false
				}

				if get.GetName() != key.Name || get.GetNamespace() != key.Namespace {
					return false
				}

				anns := get.GetAnnotations()
				return len(anns) > 0 && anns[annotationName] == "NodePort"
			}, suite.Timeout, suite.Interval).Should(BeTrue())
		})

		It("should survive deleting the service", func() {
			suite.Log.Info("deleting service")
			Expect(suite.K8sClient.Delete(ctrlCtx, svc)).Should(Succeed())

			// removed from the API server?
			Eventually(func() bool {
				get := object.New()
				get.SetGroupVersionKind(gvk)
				return apierrors.IsNotFound(suite.K8sClient.Get(ctrlCtx, client.ObjectKeyFromObject(svc), get))
			}, suite.Timeout, suite.Interval).Should(BeTrue())

		})
	})
})
