package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	itest "github.com/l7mp/dbsp/dcontroller/integration"
	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/executor"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

const (
	testLogLevel                       int8 = -4
	endpointSliceServiceViewInputTopic      = "endpointslice-controller/serviceview/input"
	endpointSliceInputTopic                 = "endpointslice-controller/endpointslice/input"
)

var suite *itest.Suite

var _ = BeforeSuite(func() {
	var err error
	suite, err = itest.NewSuite(testLogLevel)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if suite != nil {
		suite.Close()
	}
})

func TestAPIs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EndpointSlice controller example test")
}

var _ = Describe("EndpointSlice controller example", Ordered, func() {
	Context("without grouped stage", Ordered, func() {
		var (
			runner   *EndpointSliceRunner
			ctx      context.Context
			cancel   context.CancelFunc
			runnerCh chan error
			eventCh  chan EndpointViewEvent
			trace    *nodeExecutionTrace
			svc      object.Object
			es       object.Object
		)

		BeforeAll(func() {
			ctx, cancel = context.WithCancel(suite.Ctx)
			eventCh = make(chan EndpointViewEvent, 128)

			specFile := resolveSpecPath(OperatorSpec)
			var err error
			runner, err = StartEndpointSliceOperator(suite.Cfg, specFile, true, eventCh, suite.Log)
			Expect(err).NotTo(HaveOccurred())

			runnerCh = make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				runnerCh <- runner.Start(ctx)
			}()

			waitForAPIServer(runner, suite.Timeout)

			trace = newNodeExecutionTrace()
			Expect(runner.SetControllerObserver("endpointslice-controller", trace.Observer())).To(BeTrue())

			svc = itest.TestSvc.DeepCopy()
			svc.SetName("test-service-1")
			svc.SetNamespace("testnamespace")
			svc.SetAnnotations(map[string]string{EndpointSliceCtrlAnnotationName: "true"})
			Expect(unstructured.SetNestedSlice(svc.Object, []any{
				map[string]any{
					"name":       "tcp-port",
					"protocol":   "TCP",
					"port":       int64(80),
					"targetPort": int64(8080),
				},
				map[string]any{
					"name":       "udp-port",
					"protocol":   "UDP",
					"port":       int64(3478),
					"targetPort": int64(33478),
				},
			}, "spec", "ports")).NotTo(HaveOccurred())

			es = itest.TestEndpointSlice.DeepCopy()
			es.SetName("test-endpointslice-1")
			es.SetNamespace("testnamespace")
			es.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-1"})
		})

		AfterAll(func() {
			if cancel != nil {
				cancel()
			}
			Eventually(runnerCh, suite.Timeout).Should(Receive(BeNil()))

			if suite != nil {
				deleteIfExists(ctx, svc)
				deleteIfExists(ctx, es)
			}
		})

		It("emits ServiceView objects and forwards them to endpointslice input", func() {
			svcInputTap := runner.op.GetRuntime().NewSubscriber()
			svcInputTap.Subscribe(endpointSliceServiceViewInputTopic)
			defer svcInputTap.Unsubscribe(endpointSliceServiceViewInputTopic)

			Expect(suite.K8sClient.Create(ctx, svc)).To(Succeed())

			svcInputRows := 0
			Eventually(func() int {
				svcInputRows += drainTopicRows(svcInputTap.GetChannel(), endpointSliceServiceViewInputTopic)
				return svcInputRows
			}, suite.Timeout, suite.Interval).Should(BeNumerically(">", 0))

			apiServer := runner.APIServer().GetAPIServer()
			port := itest.GetPort(apiServer.GetInsecureServerAddress())

			dynamicClient, err := dynamic.NewForConfig(&rest.Config{
				Host: fmt.Sprintf("http://localhost:%d", port),
			})
			Expect(err).NotTo(HaveOccurred())

			gvr := schema.GroupVersionResource{
				Group:    viewv1a1.Group(OperatorName),
				Version:  viewv1a1.Version,
				Resource: "serviceview",
			}

			Eventually(func() []string {
				list, err := dynamicClient.Resource(gvr).Namespace("testnamespace").List(ctx, metav1.ListOptions{})
				if err != nil {
					return nil
				}

				names := make([]string, 0, len(list.Items))
				for _, item := range list.Items {
					names = append(names, item.GetName())
				}
				sort.Strings(names)
				return names
			}, suite.Timeout, suite.Interval).Should(Equal([]string{"test-service-1-0", "test-service-1-1"}))
		})

		It("emits one EndpointView object per endpoint and port", func() {
			esInputTap := runner.op.GetRuntime().NewSubscriber()
			esInputTap.Subscribe(endpointSliceInputTopic)
			defer esInputTap.Unsubscribe(endpointSliceInputTopic)

			epOutputTap := runner.op.GetRuntime().NewSubscriber()
			epOutputTap.Subscribe(endpointViewTopic)
			defer epOutputTap.Unsubscribe(endpointViewTopic)

			tokens := []string{
				"ns_0_ServiceView",
				"ns_1_EndpointSlice",
				"join_cart_1",
				"join_select",
				"op_1",
				"op_2",
				"op_3",
				"op_4",
				"op_5",
				"op_6",
				"output_endpointslice_controller_endpointview_output",
			}
			baseline := trace.Snapshot(tokens...)

			Expect(suite.K8sClient.Create(ctx, es)).To(Succeed())

			esInputRows := 0
			Eventually(func() int {
				esInputRows += drainTopicRows(esInputTap.GetChannel(), endpointSliceInputTopic)
				return esInputRows
			}, suite.Timeout, suite.Interval).Should(BeNumerically(">", 0))

			Eventually(func() int {
				return trace.SumByIDContains("ns_1_EndpointSlice")
			}, suite.Timeout, suite.Interval).Should(BeNumerically(">", baseline["ns_1_EndpointSlice"]))

			epOutputRows := waitForTopicRows(epOutputTap.GetChannel(), endpointViewTopic, 1, suite.Timeout, suite.Interval)
			Expect(epOutputRows).To(BeNumerically(
				">", 0,
			), "raw endpointview output topic saw no events; node-stage deltas: %s; node-stage totals: %s",
				trace.DeltaReport(baseline, tokens...), trace.StageReport(tokens...))

			specs := collectSpecsForService(eventCh, "test-service-1", 4, suite.Timeout)
			checkFlatSpecs(specs, []string{"192.0.2.1", "192.0.2.2"})
		})

		It("serves EndpointView objects via embedded API server", func() {
			apiServer := runner.APIServer().GetAPIServer()
			port := itest.GetPort(apiServer.GetInsecureServerAddress())

			dynamicClient, err := dynamic.NewForConfig(&rest.Config{
				Host: fmt.Sprintf("http://localhost:%d", port),
			})
			Expect(err).NotTo(HaveOccurred())

			gvr := schema.GroupVersionResource{
				Group:    viewv1a1.Group(OperatorName),
				Version:  viewv1a1.Version,
				Resource: "endpointview",
			}

			Eventually(func() int {
				list, err := dynamicClient.Resource(gvr).Namespace("testnamespace").List(ctx, metav1.ListOptions{})
				if err != nil {
					return -1
				}
				return len(list.Items)
			}, suite.Timeout, suite.Interval).Should(Equal(4))
		})

		It("updates endpoint objects when endpoint addresses change", func() {
			drainEvents(eventCh)

			esUpdate := es.DeepCopy()
			_, err := ctrlutil.CreateOrUpdate(ctx, suite.K8sClient, esUpdate, func() error {
				esUpdate.Object["endpoints"].([]any)[0].(map[string]any)["addresses"] = []any{"192.0.2.3"}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			specs := collectSpecsForService(eventCh, "test-service-1", 2, suite.Timeout)
			checkFlatSpecs(specs, []string{"192.0.2.3"})
		})
	})

	Context("with grouped stage", Ordered, func() {
		var (
			runner   *EndpointSliceRunner
			ctx      context.Context
			cancel   context.CancelFunc
			runnerCh chan error
			eventCh  chan EndpointViewEvent
			svc      object.Object
			es       object.Object
		)

		BeforeAll(func() {
			ctx, cancel = context.WithCancel(suite.Ctx)
			eventCh = make(chan EndpointViewEvent, 128)

			specFile := resolveSpecPath(OperatorGroupedSpec)
			var err error
			runner, err = StartEndpointSliceOperator(suite.Cfg, specFile, true, eventCh, suite.Log)
			Expect(err).NotTo(HaveOccurred())

			runnerCh = make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				runnerCh <- runner.Start(ctx)
			}()

			waitForAPIServer(runner, suite.Timeout)

			svc = itest.TestSvc.DeepCopy()
			svc.SetName("test-service-2")
			svc.SetNamespace("testnamespace")
			svc.SetAnnotations(map[string]string{EndpointSliceCtrlAnnotationName: "true"})
			Expect(unstructured.SetNestedSlice(svc.Object, []any{
				map[string]any{
					"name":       "tcp-port",
					"protocol":   "TCP",
					"port":       int64(80),
					"targetPort": int64(8080),
				},
				map[string]any{
					"name":       "udp-port",
					"protocol":   "UDP",
					"port":       int64(3478),
					"targetPort": int64(33478),
				},
			}, "spec", "ports")).NotTo(HaveOccurred())

			es = itest.TestEndpointSlice.DeepCopy()
			es.SetName("test-endpointslice-2")
			es.SetNamespace("testnamespace")
			es.SetLabels(map[string]string{"kubernetes.io/service-name": "test-service-2"})
		})

		AfterAll(func() {
			if cancel != nil {
				cancel()
			}
			Eventually(runnerCh, suite.Timeout).Should(Receive(BeNil()))

			if suite != nil {
				deleteIfExists(ctx, svc)
				deleteIfExists(ctx, es)
			}
		})

		It("emits grouped EndpointView objects per service port", func() {
			Expect(suite.K8sClient.Create(ctx, svc)).To(Succeed())
			Expect(suite.K8sClient.Create(ctx, es)).To(Succeed())

			specs := collectSpecsForService(eventCh, "test-service-2", 2, suite.Timeout)
			checkGatheredSpecs(specs)
		})
	})
})

func waitForAPIServer(runner *EndpointSliceRunner, timeout time.Duration) {
	GinkgoHelper()

	apiServer := runner.APIServer().GetAPIServer()
	Expect(apiServer).NotTo(BeNil())

	port := itest.GetPort(apiServer.GetInsecureServerAddress())
	Expect(port).To(BeNumerically(">", 0))

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(&rest.Config{
		Host: fmt.Sprintf("http://localhost:%d", port),
	})
	Expect(err).NotTo(HaveOccurred())

	Eventually(func() bool {
		_, err := discoveryClient.ServerGroups()
		return err == nil
	}, timeout, 100*time.Millisecond).Should(BeTrue())
}

func resolveSpecPath(specFile string) string {
	GinkgoHelper()

	if _, err := os.Stat(specFile); err == nil {
		return specFile
	}

	base := filepath.Base(specFile)
	if _, err := os.Stat(base); err == nil {
		return base
	}

	return specFile
}

func deleteIfExists(ctx context.Context, obj object.Object) {
	GinkgoHelper()

	if obj == nil {
		return
	}
	err := suite.K8sClient.Delete(ctx, obj)
	if err != nil && !errors.Is(err, os.ErrNotExist) && !apierrors.IsNotFound(err) {
		_ = err
	}
}

func drainEvents(ch <-chan EndpointViewEvent) {
	GinkgoHelper()

	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func collectSpecs(ch <-chan EndpointViewEvent, expected int, timeout time.Duration) []map[string]any {
	GinkgoHelper()

	deadline := time.After(timeout)
	set := map[string]map[string]any{}

	for len(set) < expected {
		select {
		case event := <-ch:
			if event.EventType == object.Deleted || event.Object == nil {
				continue
			}

			spec, ok, err := unstructured.NestedMap(event.Object.Object, "spec")
			if err != nil || !ok {
				continue
			}

			sortAtField([]map[string]any{spec}, "addresses")
			jsonSpec, err := json.Marshal(spec)
			if err != nil {
				continue
			}

			set[string(jsonSpec)] = spec

		case <-deadline:
			Fail(fmt.Sprintf("timed out waiting for %d EndpointView specs, got %d", expected, len(set)))
		}
	}

	ret := make([]map[string]any, 0, len(set))
	for _, spec := range set {
		ret = append(ret, spec)
	}

	return ret
}

func collectSpecsForService(ch <-chan EndpointViewEvent, serviceName string, expected int, timeout time.Duration) []map[string]any {
	GinkgoHelper()

	deadline := time.After(timeout)
	set := map[string]map[string]any{}

	for len(set) < expected {
		select {
		case event := <-ch:
			if event.EventType == object.Deleted || event.Object == nil {
				continue
			}

			spec, ok, err := unstructured.NestedMap(event.Object.Object, "spec")
			if err != nil || !ok {
				continue
			}
			if asString(spec["serviceName"]) != serviceName {
				continue
			}

			sortAtField([]map[string]any{spec}, "addresses")
			jsonSpec, err := json.Marshal(spec)
			if err != nil {
				continue
			}

			set[string(jsonSpec)] = spec

		case <-deadline:
			Fail(fmt.Sprintf("timed out waiting for %d EndpointView specs for service %q, got %d", expected, serviceName, len(set)))
		}
	}

	ret := make([]map[string]any, 0, len(set))
	for _, spec := range set {
		ret = append(ret, spec)
	}

	return ret
}

func sortAtField(specs []map[string]any, fields ...string) {
	for _, spec := range specs {
		list, ok, err := unstructured.NestedSlice(spec, fields...)
		if err != nil || !ok {
			continue
		}

		sortAny(list)
		err = unstructured.SetNestedSlice(spec, list, fields...)
		if err != nil {
			continue
		}
	}
}

func sortAny(slice []any) {
	sort.Slice(slice, func(i, j int) bool {
		jsonI, _ := json.Marshal(slice[i])
		jsonJ, _ := json.Marshal(slice[j])
		return string(jsonI) < string(jsonJ)
	})
}

func checkFlatSpecs(specs []map[string]any, addresses []string) {
	GinkgoHelper()

	Expect(specs).To(HaveLen(len(addresses) * 2))

	for _, addr := range addresses {
		Expect(hasFlatSpec(specs, addr, 80, 8080, "TCP")).To(BeTrue(), "missing expected flat EndpointView spec for address %s/TCP", addr)
		Expect(hasFlatSpec(specs, addr, 3478, 33478, "UDP")).To(BeTrue(), "missing expected flat EndpointView spec for address %s/UDP", addr)
	}
}

func checkGatheredSpecs(specs []map[string]any) {
	GinkgoHelper()

	Expect(specs).To(HaveLen(2))
	sortAtField(specs, "addresses")

	addresses := []string{"192.0.2.1", "192.0.2.2"}
	Expect(hasGatheredSpec(specs, addresses, 80, 8080, "TCP")).To(BeTrue(), "missing expected grouped EndpointView TCP spec")
	Expect(hasGatheredSpec(specs, addresses, 3478, 33478, "UDP")).To(BeTrue(), "missing expected grouped EndpointView UDP spec")
}

func hasFlatSpec(specs []map[string]any, address string, port, targetPort int64, protocol string) bool {
	for _, spec := range specs {
		if asString(spec["serviceName"]) != "test-service-1" {
			continue
		}
		if asString(spec["type"]) != "ClusterIP" {
			continue
		}
		if asString(spec["protocol"]) != protocol {
			continue
		}
		if asString(spec["address"]) != address {
			continue
		}
		if !sameInt(spec["port"], port) || !sameInt(spec["targetPort"], targetPort) {
			continue
		}

		return true
	}

	return false
}

func hasGatheredSpec(specs []map[string]any, addresses []string, port, targetPort int64, protocol string) bool {
	want := append([]string(nil), addresses...)
	sort.Strings(want)

	for _, spec := range specs {
		if asString(spec["serviceName"]) != "test-service-2" {
			continue
		}
		if asString(spec["type"]) != "ClusterIP" {
			continue
		}
		if asString(spec["protocol"]) != protocol {
			continue
		}
		if !sameInt(spec["port"], port) || !sameInt(spec["targetPort"], targetPort) {
			continue
		}

		got := stringSlice(spec["addresses"])
		sort.Strings(got)
		if len(got) != len(want) {
			continue
		}

		matched := true
		for i := range got {
			if got[i] != want[i] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}

	return false
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func sameInt(v any, want int64) bool {
	got, ok := toInt64(v)
	return ok && got == want
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case float32:
		f := float64(n)
		if math.Trunc(f) != f {
			return 0, false
		}
		return int64(f), true
	case float64:
		if math.Trunc(n) != n {
			return 0, false
		}
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

func stringSlice(v any) []string {
	list, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(list))
	for _, item := range list {
		s, ok := item.(string)
		if !ok {
			return nil
		}
		out = append(out, s)
	}

	return out
}

func drainTopicRows(ch <-chan dbspruntime.Event, topic string) int {
	rows := 0

	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return rows
			}
			if event.Name != topic {
				continue
			}
			rows += event.Data.Size()
		default:
			return rows
		}
	}
}

func waitForTopicRows(ch <-chan dbspruntime.Event, topic string, minRows int, timeout, interval time.Duration) int {
	deadline := time.Now().Add(timeout)
	rows := 0

	for rows < minRows {
		rows += drainTopicRows(ch, topic)
		if rows >= minRows {
			return rows
		}
		if time.Now().After(deadline) {
			return rows
		}
		time.Sleep(interval)
	}

	return rows
}

type nodeExecutionTrace struct {
	mu     sync.Mutex
	totals map[string]int
}

func newNodeExecutionTrace() *nodeExecutionTrace {
	return &nodeExecutionTrace{totals: map[string]int{}}
}

func (t *nodeExecutionTrace) Observer() executor.ObserverFunc {
	if t == nil {
		return nil
	}

	return func(node *circuit.Node, values map[string]zset.ZSet, _ []string, _ int) {
		if node == nil {
			return
		}

		v, ok := values[node.ID]
		if !ok || v.IsZero() {
			return
		}

		t.mu.Lock()
		t.totals[node.ID] += v.Size()
		t.mu.Unlock()
	}
}

func (t *nodeExecutionTrace) SumByIDContains(token string) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	total := 0
	for id, rows := range t.totals {
		if strings.Contains(id, token) {
			total += rows
		}
	}

	return total
}

func (t *nodeExecutionTrace) StageReport(tokens ...string) string {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(tokens) == 0 {
		return ""
	}

	parts := make([]string, 0, len(tokens))
	for _, token := range tokens {
		total := 0
		for id, rows := range t.totals {
			if strings.Contains(id, token) {
				total += rows
			}
		}
		parts = append(parts, fmt.Sprintf("%s=%d", token, total))
	}

	return strings.Join(parts, ", ")
}

func (t *nodeExecutionTrace) Snapshot(tokens ...string) map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	ret := make(map[string]int, len(tokens))
	for _, token := range tokens {
		total := 0
		for id, rows := range t.totals {
			if strings.Contains(id, token) {
				total += rows
			}
		}
		ret[token] = total
	}

	return ret
}

func (t *nodeExecutionTrace) DeltaReport(before map[string]int, tokens ...string) string {
	t.mu.Lock()
	defer t.mu.Unlock()

	parts := make([]string, 0, len(tokens))
	for _, token := range tokens {
		total := 0
		for id, rows := range t.totals {
			if strings.Contains(id, token) {
				total += rows
			}
		}
		parts = append(parts, fmt.Sprintf("%s=%d", token, total-before[token]))
	}

	return strings.Join(parts, ", ")
}
