package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dbsp/connectors/kubernetes"
	kruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/spec"
	"github.com/l7mp/dbsp/engine/zset"
)

// The big apply: a desired-state loop over ConfigMaps (a watch projected
// to the desired shape, the Reconciler on the output, the Updater behind
// it) is handed its whole desired set in one step, and once it has
// converged the test overwrites half of the objects and plants as many
// foreign ones with the owned label at full speed, the way a stray
// `kubectl apply -f` does. The loop re-emits every outstanding correction
// each step until the watch confirms it; the write pump's hold absorbs
// those re-emissions, so the measure is writes per object the loop
// actually owes. The run takes a while and its numbers depend on the
// machine, so it only runs when DBSP_BIG_APPLY is set: to the number of
// objects, or to "plain" for the plain actuator over the default size.
var _ = Describe("Big apply over envtest", func() {
	It("writes each correction about once", func() {
		setting := os.Getenv("DBSP_BIG_APPLY")
		if setting == "" {
			Skip("set DBSP_BIG_APPLY=<objects> (or plain) to run the big apply")
		}
		n, plain := 1000, false
		if setting == "plain" {
			plain = true
		} else if v, err := strconv.Atoi(setting); err == nil {
			n = v
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ns := suite.Namespace
		labels := map[string]string{"bigapply": "owned"}

		// Every write the connectors issue goes through this transport.
		var writes, creates, updates, deletes atomic.Int64
		cfg := rest.CopyConfig(suite.Cfg)
		cfg.QPS, cfg.Burst = 500, 1000
		cfg.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
			return roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/configmaps") {
					switch req.Method {
					case http.MethodPost:
						creates.Add(1)
						writes.Add(1)
					case http.MethodPut, http.MethodPatch:
						updates.Add(1)
						writes.Add(1)
					case http.MethodDelete:
						deletes.Add(1)
						writes.Add(1)
					}
				}
				return rt.RoundTrip(req)
			})
		}
		krt, err := kruntime.New(kruntime.Config{RESTConfig: cfg, PlainWrites: plain, Logger: logr.Discard()})
		Expect(err).NotTo(HaveOccurred())
		go func() { _ = krt.Start(ctx) }()

		rt := dbspruntime.NewRuntime("bigapply", logr.Discard())
		var errs []error
		var errMu sync.Mutex
		errCh := make(chan dbspruntime.Error, 1024)
		rt.SetErrorChannel(errCh)
		go func() {
			for e := range errCh {
				errMu.Lock()
				errs = append(errs, e)
				errMu.Unlock()
			}
		}()
		reg := dbspruntime.NewConnectorRegistry()
		Expect(reg.Register(kubernetes.NewFactory(kubernetes.Env{
			Runtime: func() (*kruntime.Runtime, error) { return krt, nil },
		}))).To(Succeed())
		Expect(reg.Register(dbspruntime.ConnectorFactory{
			Name:   "driver",
			Groups: []string{"driver.connector.dcontroller.io"},
			NewSource: func(rt *dbspruntime.Runtime, s *spec.Source, topic string) (dbspruntime.Runnable, error) {
				return &waiter{name: "driver/" + topic}, nil
			},
		})).To(Succeed())

		var op spec.RuntimeSpec
		Expect(json.Unmarshal([]byte(fmt.Sprintf(`{
		  "sources": [
		    {"apiGroup": "driver.connector.dcontroller.io", "kind": "Desired"},
		    {"apiGroup": "", "version": "v1", "kind": "ConfigMap", "namespace": %q,
		     "labelSelector": {"matchLabels": {"bigapply": "owned"}}}
		  ],
		  "circuits": [
		    {
		      "name": "input",
		      "inputs": ["ConfigMap"],
		      "outputs": ["Observed"],
		      "pipeline": [[{"@inputs": ["ConfigMap"]},
		        {"@project": {"apiVersion": "$.apiVersion", "kind": "$.kind",
		          "metadata": {"name": "$.metadata.name", "namespace": "$.metadata.namespace",
		                       "labels": "$.metadata.labels"},
		          "data": "$.data"}},
		        {"@output": "Observed"}]],
		      "transforms": [{"name": "Incrementalizer"}]
		    },
		    {
		      "name": "loop",
		      "inputs": ["Desired", "Observed"],
		      "outputs": ["Out"],
		      "pipeline": [[{"@inputs": ["Desired"]}, {"@project": {"$.": "$."}}, {"@output": "Out"}]],
		      "transforms": [{"name": "Reconciler", "pairs": [["Observed", "Out"]]},
		                     {"name": "Incrementalizer"}]
		    }
		  ],
		  "targets": [
		    {"apiGroup": "", "version": "v1", "kind": "ConfigMap", "type": "Updater", "as": "Out"}
		  ]
		}`, ns)), &op)).To(Succeed())
		Expect(rt.Load(&op, reg)).To(Succeed())

		go func() { _ = rt.Start(ctx) }()
		time.Sleep(time.Second)

		desired := func(i int, v string) *unstructured.Unstructured {
			return unstructured.New(map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("big-%05d", i),
					"namespace": ns,
					"labels":    map[string]any{"bigapply": "owned"},
				},
				"data": map[string]any{"k": v},
			})
		}
		owned := func() (ok, foreign int) {
			list := &corev1.ConfigMapList{}
			Expect(suite.K8sClient.List(ctx, list, client.InNamespace(ns), client.MatchingLabels(labels))).To(Succeed())
			for _, cm := range list.Items {
				if strings.HasPrefix(cm.Name, "big-") && cm.Data["k"] == "desired" {
					ok++
				} else {
					foreign++
				}
			}
			return ok, foreign
		}
		// converge waits for the plant to match the desired set, reporting
		// progress and the runtime's errors on the way.
		converge := func(phase string) {
			deadline := time.Now().Add(30*time.Second + time.Duration(n)*50*time.Millisecond)
			for {
				ok, foreign := owned()
				errMu.Lock()
				nerr := len(errs)
				var last error
				if nerr > 0 {
					last = errs[nerr-1]
				}
				errMu.Unlock()
				GinkgoWriter.Printf("  %s: ok=%d/%d foreign=%d writes=%d errors=%d last=%v\n", phase, ok, n, foreign, writes.Load(), nerr, last)
				if ok == n && foreign == 0 {
					return
				}
				if time.Now().After(deadline) {
					Fail(fmt.Sprintf("%s did not converge: ok=%d/%d foreign=%d writes=%d errors=%d last=%v", phase, ok, n, foreign, writes.Load(), nerr, last))
				}
				time.Sleep(2 * time.Second)
			}
		}

		// Phase 1: the whole desired set in one step.
		all := zset.New()
		for i := range n {
			all.Insert(desired(i, "desired"), 1)
		}
		start := time.Now()
		Expect(rt.NewPublisher().Publish(dbspruntime.Event{Name: "Desired", Data: all})).To(Succeed())
		converge("phase 1")
		w1, t1 := writes.Load(), time.Since(start)

		// Phase 2: the stray apply. Half the owned objects are overwritten,
		// as many foreign objects are planted under the owned label, at
		// full speed from sixteen workers.
		start = time.Now()
		var wg sync.WaitGroup
		work := make(chan int, n)
		for i := range n {
			work <- i
		}
		close(work)
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer GinkgoRecover()
				for i := range work {
					if i%2 == 0 {
						cm := &corev1.ConfigMap{}
						key := client.ObjectKey{Namespace: ns, Name: fmt.Sprintf("big-%05d", i)}
						Expect(suite.K8sClient.Get(ctx, key, cm)).To(Succeed())
						cm.Data["k"] = "stray"
						Expect(client.IgnoreNotFound(suite.K8sClient.Update(ctx, cm))).To(Succeed())
					} else {
						cm := &corev1.ConfigMap{
							ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("foreign-%05d", i), Namespace: ns, Labels: labels},
							Data:       map[string]string{"k": "stray"},
						}
						Expect(suite.K8sClient.Create(ctx, cm)).To(Succeed())
					}
				}
			}()
		}
		wg.Wait()
		applied := time.Since(start)
		converge("phase 2")
		w2, t2 := writes.Load()-w1, time.Since(start)

		// Let the loop settle: a healthy loop issues nothing more once
		// the watch has confirmed everything.
		time.Sleep(3 * kruntimeHold)
		w3 := writes.Load() - w1 - w2

		errMu.Lock()
		nerr := len(errs)
		errMu.Unlock()
		GinkgoWriter.Printf("\nbig apply: n=%d plain=%v\n", n, plain)
		GinkgoWriter.Printf("  phase 1 (desired set):  %d writes for %d objects (%.2f per object) in %s\n", w1, n, float64(w1)/float64(n), t1.Round(time.Millisecond))
		GinkgoWriter.Printf("  phase 2 (stray apply):  %d writes for %d corrections (%.2f per object) in %s, the apply itself took %s\n", w2, n, float64(w2)/float64(n), t2.Round(time.Millisecond), applied.Round(time.Millisecond))
		GinkgoWriter.Printf("  settled:                %d writes after convergence; creates=%d updates=%d deletes=%d; runtime errors=%d\n", w3, creates.Load(), updates.Load(), deletes.Load(), nerr)

		if !plain {
			Expect(float64(w1)).To(BeNumerically("<=", 1.5*float64(n)), "phase 1 writes per object")
			Expect(float64(w2)).To(BeNumerically("<=", 1.5*float64(n)), "phase 2 writes per object")
			Expect(w3).To(BeZero(), "writes after convergence")
		}
	})
})

// kruntimeHold mirrors the connector's write hold: the time a written
// object absorbs re-emissions of the same target.
const kruntimeHold = 5 * time.Second

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// waiter is a source that produces nothing: the test publishes on its
// stream directly.
type waiter struct{ name string }

func (w *waiter) Name() string                    { return w.name }
func (w *waiter) Start(ctx context.Context) error { <-ctx.Done(); return nil }
