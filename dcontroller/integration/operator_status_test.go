package integration

import (
	"context"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	"github.com/l7mp/dbsp/dcontroller/operator"
)

var _ = Describe("Operator status report test:", Ordered, func() {
	Context("When creating a controller with an invalid config", Ordered, Label("controller"), func() {
		var (
			suite      *Suite
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
		)

		BeforeAll(func() {
			var err error
			suite, err = NewSuite(loglevel, filepath.Join("..", "config", "crd", "resources"))
			Expect(err).NotTo(HaveOccurred())

			ctrlCtx, ctrlCancel = context.WithCancel(suite.Ctx)
		})

		AfterAll(func() {
			ctrlCancel()
			suite.Close()
		})

		It("should create and start the operator controller", func() {
			suite.Log.Info("setting up operator controller")
			c, err := operator.NewOperatorController(k8sruntime.Config{RESTConfig: suite.Cfg, Logger: suite.Log})
			Expect(err).NotTo(HaveOccurred())

			suite.Log.Info("starting operator controller")
			go func() {
				defer GinkgoRecover()
				err := c.Start(ctrlCtx)
				Expect(err).ToNot(HaveOccurred(), "failed to run controller")
			}()
		})

		It("should let an operator to be attached to the manager", func() {
			yamlData := `
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: bogus-operator
spec:
  controllers:
    - name: bogus-controller
      sources: []
      pipeline:
        - '@select': whatever
      targets:
        - kind: myview`

			var op opv1a1.Operator
			Expect(yaml.Unmarshal([]byte(yamlData), &op)).NotTo(HaveOccurred())

			suite.Log.Info("adding new operator")
			Expect(suite.K8sClient.Create(ctrlCtx, &op)).Should(Succeed())
		})

		It("should set the accepted operator status to False", func() {
			key := types.NamespacedName{Name: "bogus-operator"}
			var status opv1a1.OperatorStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctrlCtx, key, get)
				if err != nil {
					return false
				}
				cond := meta.FindStatusCondition(get.Status.Conditions, string(opv1a1.OperatorConditionReady))
				if cond == nil {
					return false
				}
				status = get.Status
				return cond.Status == metav1.ConditionFalse
			}, suite.Timeout, suite.Interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Type).Should(
				Equal(string(opv1a1.OperatorConditionReady)))
			Expect(cond.Status).Should(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.OperatorReasonNotReady)))
		})

		It("should survive deleting the operator", func() {
			suite.Log.Info("deleting op")
			op := opv1a1.Operator{}
			op.SetName("bogus-operator")
			Expect(suite.K8sClient.Delete(ctrlCtx, &op)).Should(Succeed())
		})
	})

	// write container-num into pods
	Context("When applying a controller with a pipeline that makes a runtime error", Ordered, Label("controller"), func() {
		var (
			suite      *Suite
			ctrlCtx    context.Context
			ctrlCancel context.CancelFunc
			pod        object.Object
			op         opv1a1.Operator
		)

		BeforeAll(func() {
			var err error
			suite, err = NewSuite(loglevel, filepath.Join("..", "config", "crd", "resources"))
			Expect(err).NotTo(HaveOccurred())

			ctrlCtx, ctrlCancel = context.WithCancel(suite.Ctx)
			pod = TestPod.DeepCopy()
		})

		AfterAll(func() {
			ctrlCancel()
			suite.Close()
		})

		It("should create and start the operator controller", func() {
			suite.Log.Info("setting up operator controller")
			c, err := operator.NewOperatorController(k8sruntime.Config{RESTConfig: suite.Cfg, Logger: suite.Log})
			Expect(err).NotTo(HaveOccurred())

			suite.Log.Info("starting operator controller")
			go func() {
				defer GinkgoRecover()
				err := c.Start(ctrlCtx)
				Expect(err).ToNot(HaveOccurred(), "failed to run controller")
			}()
		})

		It("should let an operator to be attached to the manager", func() {
			yamlData := `
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: pod-container-num-annotator
spec:
  controllers:
    - name: pod-container-num-annotator
      sources:
        - apiGroup: ""
          kind: Pod
      pipeline:
        - "@project":                                
            metadata:                                
              name: "$.metadata.name"                # copy metadata.namespace and metadata.name
              namespace: "$.metadata.namespace"      
              annotations:                           
                "dcontroller.io/container-num":      # add a new annotation indicating the number of containers
                  '@len': ["$.spec.containers"]      # this is deliberately wrong (needs string coercion)
      targets:
        - apiGroup: ""
          kind: Pod
          type: Patcher`
			Expect(yaml.Unmarshal([]byte(yamlData), &op)).NotTo(HaveOccurred())

			suite.Log.Info("adding new operator")
			Expect(suite.K8sClient.Create(ctrlCtx, &op)).Should(Succeed())
		})

		It("should create a nonempty status on the operator resource", func() {
			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.OperatorStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctrlCtx, key, get)
				if err != nil {
					return false
				}
				cond := meta.FindStatusCondition(get.Status.Conditions, string(opv1a1.OperatorConditionReady))
				if cond == nil {
					return false
				}
				status = get.Status
				return cond.Status == metav1.ConditionTrue
			}, suite.Timeout, suite.Interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Type).Should(
				Equal(string(opv1a1.OperatorConditionReady)))
			Expect(cond.Status).Should(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.OperatorReasonReady)))
		})

		It("should keep operator ready status when a runtime error happens", func() {
			suite.Log.Info("adding pod")
			Expect(suite.K8sClient.Create(ctrlCtx, pod)).Should(Succeed())

			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.OperatorStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctrlCtx, key, get)
				if err != nil {
					return false
				}
				status = get.Status
				cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
				if cond == nil {
					return false
				}
				return cond.Status == metav1.ConditionTrue
			}, suite.Timeout, suite.Interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).Should(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.OperatorReasonReady)))
		})

		It("should report a true ready status for the correct operator spec", func() {
			yamlData := `
apiVersion: dcontroller.io/v1alpha1
kind: Operator
metadata:
  name: pod-container-num-annotator
spec:
  controllers:
    - name: pod-container-num-annotator
      sources:
        - apiGroup: ""
          kind: Pod
      pipeline:
        - "@project":                                
            metadata:                                
              name: "$.metadata.name"                # copy metadata.namespace and metadata.name
              namespace: "$.metadata.namespace"      
              annotations:                           
                "dcontroller.io/container-num":      # add a new annotation indicating the number of containers
                  '@string':                         # explicitly force string conversion
                    '@len': ["$.spec.containers"]
      targets:
        - apiGroup: ""
          kind: Pod
          type: Patcher`
			newOp := opv1a1.Operator{}
			Expect(yaml.Unmarshal([]byte(yamlData), &newOp)).NotTo(HaveOccurred())

			suite.Log.Info("updating operator")
			_, err := ctrlutil.CreateOrUpdate(ctrlCtx, suite.K8sClient, &op, func() error {
				newOp.Spec.DeepCopyInto(&op.Spec)
				return nil
			})
			Expect(err).Should(Succeed())

			key := types.NamespacedName{Name: "pod-container-num-annotator"}
			var status opv1a1.OperatorStatus
			Eventually(func() bool {
				get := &opv1a1.Operator{}
				err := suite.K8sClient.Get(ctrlCtx, key, get)
				if err != nil {
					return false
				}
				status = get.Status
				cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
				if cond == nil {
					return false
				}
				return cond.Status == metav1.ConditionTrue
			}, suite.Timeout, suite.Interval).Should(BeTrue())

			cond := meta.FindStatusCondition(status.Conditions, string(opv1a1.OperatorConditionReady))
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).Should(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).Should(
				Equal(string(opv1a1.OperatorReasonReady)))
		})

		It("should survive deleting the pod", func() {
			suite.Log.Info("deleting pod")
			Expect(suite.K8sClient.Delete(ctrlCtx, pod)).Should(Succeed())
		})

		It("should survive deleting the operator", func() {
			suite.Log.Info("deleting op")
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				return suite.K8sClient.Delete(ctrlCtx, &op)
			})
			Expect(err).Should(Succeed())
		})
	})
})
